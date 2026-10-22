#!/usr/bin/env python3
"""Incremental public Xueqiu timeline -> durable outbox -> SMTP. Standard library only."""
import argparse
import contextlib
import fcntl
import hashlib
import html
from html.parser import HTMLParser
import http.cookiejar
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import re
import shlex
import smtplib
import sqlite3
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from email.message import EmailMessage
from email.utils import formatdate
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent
RUNTIME = Path(os.environ.get('MONITOR_RUNTIME', str(ROOT / 'runtime')))
SETTINGS_PATH = RUNTIME / 'settings.json'
LOCAL = json.loads(SETTINGS_PATH.read_text()) if SETTINGS_PATH.exists() else {}
USER_ID = os.environ.get('XUEQIU_USER_ID', '2151206013')
NAME = os.environ.get('XUEQIU_NAME', 'terryp57')
RECIPIENT = os.environ.get('MONITOR_RECIPIENT') or LOCAL.get('recipient', '')
SMTP_USER = os.environ.get('SMTP_USERNAME') or LOCAL.get('smtp_username', '') or RECIPIENT
PROFILE = f'https://www.xueqiu.com/u/{USER_ID}'
SMTP_ENV = Path(LOCAL['smtp_env']) if LOCAL.get('smtp_env') else None
TZ = ZoneInfo('Asia/Shanghai')
MAX_PAGES = 50
LOOKBACK_MS = 12 * 3600_000


def in_active_window(now_ms):
    local = datetime.fromtimestamp(now_ms / 1000, TZ)
    return 6 <= local.hour < 23 or (local.hour == 23 and local.minute == 0)


class MonitorError(Exception):
    pass


class TextExtractor(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self.hidden = 0

    def handle_starttag(self, tag, attrs):
        if tag in ('script', 'style'):
            self.hidden += 1
        if tag in ('br', 'p', 'div', 'blockquote', 'li'):
            self.parts.append('\n')
        if tag == 'img':
            attrs = dict(attrs)
            src = attrs.get('src', '')
            if src.startswith(('https://', 'http://')):
                self.parts.append(f'\n[图片] {src}\n')

    def handle_endtag(self, tag):
        if tag in ('script', 'style'):
            self.hidden = max(0, self.hidden - 1)
        if tag in ('p', 'div', 'blockquote', 'li'):
            self.parts.append('\n')

    def handle_data(self, data):
        if not self.hidden:
            self.parts.append(data)


def plain(value):
    p = TextExtractor()
    p.feed(str(value or ''))
    return re.sub(r'\n[ \t]*\n(?:[ \t]*\n)+', '\n\n', ''.join(p.parts)).strip()


def stamp(ms=None):
    return datetime.fromtimestamp((ms / 1000) if ms else time.time(), TZ).strftime('%Y-%m-%d %H:%M:%S')


def init_db(path):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA journal_mode=WAL')
    db.executescript('''
      CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS posts (
        id TEXT PRIMARY KEY, created_ms INTEGER NOT NULL, payload TEXT NOT NULL,
        delivery TEXT NOT NULL, discovered_ms INTEGER NOT NULL, sent_ms INTEGER
      );
    ''')
    return db


def get_meta(db, key, default=None):
    row = db.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()
    return row[0] if row else default


def put_meta(db, key, value):
    db.execute('INSERT OR REPLACE INTO meta VALUES (?,?)', (key, str(value)))


def normalize(s):
    if str(s.get('user_id', s.get('user', {}).get('id', ''))) != USER_ID:
        raise MonitorError('unexpected_author')
    sid = str(s.get('id', ''))
    ms = int(s.get('created_at', 0))
    if not sid.isdigit() or ms < 1000000000000:
        raise MonitorError('invalid_post_identity')
    text = plain(s.get('text') or s.get('description'))
    quoted = s.get('retweeted_status')
    if isinstance(quoted, dict):
        q = plain(quoted.get('text') or quoted.get('description'))
        if q:
            qname = quoted.get('user', {}).get('screen_name', '原帖')
            text += f'\n\n引用 @{qname}：\n{q}'
    return {'id': sid, 'created_ms': ms, 'title': plain(s.get('title')),
            'text': text, 'url': f'https://xueqiu.com/{USER_ID}/{sid}',
            'truncated': bool(s.get('truncated'))}


class Xueqiu:
    def __init__(self):
        # Visitor cookies stay in memory; never read the user's browser cookie store.
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
        self.opener.addheaders = [('User-Agent', 'Mozilla/5.0'), ('Referer', PROFILE)]
        self.read(PROFILE, expect_json=False)

    def read(self, url, expect_json=True):
        try:
            with self.opener.open(url, timeout=25) as r:
                if urllib.parse.urlparse(r.url).hostname not in ('www.xueqiu.com', 'xueqiu.com'):
                    raise MonitorError('unexpected_source_redirect')
                body = r.read(8_000_000)
            if not expect_json:
                if f'"id":{USER_ID}' not in body.decode('utf-8', 'replace'):
                    raise MonitorError('profile_not_available')
                return None
            data = json.loads(body)
            if not isinstance(data, dict) or not isinstance(data.get('statuses'), list):
                raise MonitorError('timeline_not_available')
            return data
        except urllib.error.HTTPError as e:
            raise MonitorError(f'xueqiu_http_{e.code}') from None
        except (urllib.error.URLError, TimeoutError, OSError):
            raise MonitorError('xueqiu_network_error') from None
        except (ValueError, UnicodeError):
            raise MonitorError('xueqiu_non_json_response') from None

    def fetch(self, watermark=None):
        result = {}
        for page in range(1, MAX_PAGES + 1):
            params = urllib.parse.urlencode({'user_id': USER_ID, 'page': page, 'type': 10, 'count': 20})
            data = self.read('https://www.xueqiu.com/v4/statuses/user_timeline.json?' + params)
            if int(data.get('page', page)) != page:
                raise MonitorError('unexpected_page')
            posts = []
            for raw in data['statuses']:
                p = normalize(raw)
                pinned = raw.get('mark') == 1
                if not pinned:
                    posts.append(p)
                # New pinned posts must be sent too; old pins cannot end catch-up early.
                if not pinned or watermark is None or p['created_ms'] >= watermark:
                    result[p['id']] = p
            if page == 1 and not posts:
                # Empty/hidden feeds must never silently replace a working baseline.
                raise MonitorError('empty_timeline')
            if watermark is None or not posts:
                break
            if min(p['created_ms'] for p in posts) < watermark:
                break
            max_page = int(data.get('maxPage') or 0)
            if max_page and page >= max_page:
                break
            if page == MAX_PAGES:
                raise MonitorError('catchup_page_limit')
            time.sleep(1)
        return sorted(result.values(), key=lambda p: (p['created_ms'], int(p['id'])))


def ingest(db, posts, now_ms):
    initialized = get_meta(db, 'initialized_ms')
    baseline = initialized is None
    watermark = int(get_meta(db, 'watermark_ms', 0))
    new_count = 0
    with db:
        for p in posts:
            # Keep original baseline posts for audit; never send historical backfill.
            cutoff = max(int(initialized or now_ms), now_ms - LOOKBACK_MS)
            eligible = not baseline and cutoff <= p['created_ms'] <= now_ms
            state = 'pending' if eligible else 'baseline'
            cur = db.execute('INSERT OR IGNORE INTO posts VALUES (?,?,?,?,?,NULL)',
                             (p['id'], p['created_ms'], json.dumps(p, ensure_ascii=False), state, now_ms))
            new_count += int(eligible and cur.rowcount == 1)
        if baseline:
            put_meta(db, 'initialized_ms', now_ms)
            put_meta(db, 'watermark_ms', now_ms)
        elif posts:
            put_meta(db, 'watermark_ms', max(watermark, max(p['created_ms'] for p in posts)))
        put_meta(db, 'last_fetch_ms', now_ms)
        put_meta(db, 'fetch_failures', 0)
        put_meta(db, 'backoff_until_ms', 0)
    return {'baseline': baseline, 'fetched': len(posts), 'new': new_count}


def smtp_password():
    # Parse just the existing required variable. Do not execute an env file as code.
    configured = os.environ.get('SMTP_PASSWORD') or os.environ.get('MARKET_REPORT_SMTP_PASSWORD')
    if configured:
        return configured
    if SMTP_ENV is None:
        raise MonitorError('smtp_credential_missing')
    for line in SMTP_ENV.read_text().splitlines():
        tokens = shlex.split(line, comments=True, posix=True)
        if tokens and tokens[0] == 'export':
            tokens = tokens[1:]
        if tokens and tokens[0].startswith('MARKET_REPORT_SMTP_PASSWORD='):
            value = tokens[0].split('=', 1)[1]
            if value:
                return value
    raise MonitorError('smtp_credential_missing')


def send_mail(subject, body, message_key):
    message = EmailMessage()
    if not RECIPIENT or not SMTP_USER:
        raise MonitorError('smtp_identity_missing')
    message['From'] = SMTP_USER
    message['To'] = RECIPIENT
    message['Subject'] = subject
    message['Date'] = formatdate(localtime=True)
    digest = hashlib.sha256(message_key.encode()).hexdigest()[:32]
    message['Message-ID'] = f'<xueqiu-{digest}@126.com>'
    message.set_content(body)
    message.add_alternative('<html><body><pre style="white-space:pre-wrap;font:16px/1.6 sans-serif">'
                            + html.escape(body) + '</pre></body></html>', subtype='html')
    try:
        password = smtp_password()
        with smtplib.SMTP_SSL('smtp.126.com', 465, timeout=30, context=ssl.create_default_context()) as server:
            server.login(SMTP_USER, password)
            rejected = server.send_message(message)
            if rejected:
                raise MonitorError('smtp_recipient_rejected')
    except smtplib.SMTPResponseException as e:
        raise MonitorError(f'smtp_error_{e.smtp_code}') from None
    except (OSError, smtplib.SMTPException):
        raise MonitorError('smtp_network_or_protocol_error') from None


def render_posts(posts):
    blocks = []
    for p in posts:
        blocks.append(f"{NAME} · {stamp(p['created_ms'])}\n{p['title']}\n{p['text']}\n"
                      + ('[列表可能截断，请打开原帖查看全文]\n' if p.get('truncated') else '')
                      + f"原帖：{p['url']}")
    return '\n\n' + ('\n\n' + '—' * 30 + '\n\n').join(blocks)


def deliver(db, sender=send_mail, now_ms=None):
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    if not in_active_window(now_ms):
        return 0
    with db:
        db.execute("UPDATE posts SET delivery='expired' WHERE delivery='pending' AND created_ms<?",
                   (now_ms - LOOKBACK_MS,))
    rows = db.execute("SELECT id,payload FROM posts WHERE delivery='pending' ORDER BY created_ms,id").fetchall()
    if not rows:
        return 0
    posts = [json.loads(row['payload']) for row in rows]
    ids = [row['id'] for row in rows]
    body = f'检测到 {NAME} 的 {len(posts)} 条新动态。时间均为北京时间。' + render_posts(posts)
    sender(f'[雪球动态] {NAME} · {len(posts)} 条新发言', body, 'posts:' + ','.join(ids))
    now = int(time.time() * 1000)
    with db:
        db.executemany("UPDATE posts SET delivery='sent',sent_ms=? WHERE id=?", [(now, sid) for sid in ids])
        put_meta(db, 'last_delivery_ms', now)
    return len(rows)


def status(db):
    meta = dict(db.execute('SELECT key,value FROM meta'))
    counts = dict(db.execute('SELECT delivery,COUNT(*) FROM posts GROUP BY delivery'))
    return {'user': NAME, 'user_id': USER_ID, 'recipient': RECIPIENT, 'interval_seconds': 300,
            'timezone': 'Asia/Shanghai', 'active_window': '06:00-23:00 (last minute 23:00)',
            'lookback_hours': 12,
            'meta': meta, 'counts': counts, 'reported_at': stamp()}


def write_status(db):
    tmp = RUNTIME / 'status.tmp'
    tmp.write_text(json.dumps(status(db), ensure_ascii=False, indent=2))
    tmp.replace(RUNTIME / 'status.json')


def check(db):
    now = int(time.time() * 1000)
    if not in_active_window(now):
        with db:
            put_meta(db, 'last_window_skip_ms', now)
        logging.info('outside_active_window')
        return
    acquisition_error = None
    try:
        if now >= int(get_meta(db, 'backoff_until_ms', 0)):
            posts = Xueqiu().fetch(now - LOOKBACK_MS)
            result = ingest(db, posts, now)
            with db:
                put_meta(db, 'last_error', '')
            logging.info('fetch_ok %s', json.dumps(result))
        else:
            logging.info('acquisition_backoff')
    except Exception as e:
        acquisition_error = str(e) if isinstance(e, MonitorError) else type(e).__name__
        with db:
            failures = int(get_meta(db, 'fetch_failures', 0)) + 1
            put_meta(db, 'fetch_failures', failures)
            put_meta(db, 'last_error', acquisition_error)
            if failures >= 3:
                put_meta(db, 'backoff_until_ms', now + 30 * 60_000)
        logging.error('fetch_failed code=%s failures=%s', acquisition_error, failures)
    try:
        sent = deliver(db)
        logging.info('delivery_ok sent=%s', sent)
        with db:
            put_meta(db, 'delivery_error', '')
    except Exception as e:
        error = str(e) if isinstance(e, MonitorError) else type(e).__name__
        with db:
            put_meta(db, 'delivery_error', error)
        logging.error('delivery_failed code=%s pending_preserved=true', error)
        raise MonitorError(error) from None
    if acquisition_error:
        if (in_active_window(int(time.time() * 1000))
                and int(get_meta(db, 'fetch_failures', 0)) >= 3
                and now - int(get_meta(db, 'last_alert_ms', 0)) > 6 * 3600_000):
            send_mail('[雪球监控异常] terryp57 动态暂时无法获取',
                      f'连续获取失败，错误类型：{acquisition_error}。\n已保留去重记录与待发送队列，30 分钟后重试。\n'
                      f'请查看本机监控运行状态。时间：{stamp()}。', f'alert:{now}')
            with db:
                put_meta(db, 'last_alert_ms', now)
        raise MonitorError(acquisition_error)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['check', 'status', 'test-mail'])
    args = parser.parse_args()
    os.umask(0o077)
    RUNTIME.mkdir(parents=True, exist_ok=True, mode=0o700)
    handler = RotatingFileHandler(RUNTIME / 'monitor.log', maxBytes=2_000_000, backupCount=3)
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logging.basicConfig(level=logging.INFO, handlers=[handler])
    with (RUNTIME / 'monitor.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print('already_running')
            return 0
        with contextlib.closing(init_db(RUNTIME / 'state.sqlite3')) as db:
            try:
                if args.action == 'check':
                    check(db)
                elif args.action == 'test-mail':
                    if not in_active_window(int(time.time() * 1000)):
                        raise MonitorError('test_mail_outside_active_window')
                    if not get_meta(db, 'initialized_ms'):
                        raise MonitorError('baseline_not_initialized')
                    n = db.execute('SELECT COUNT(*) FROM posts').fetchone()[0]
                    row = db.execute('SELECT payload FROM posts ORDER BY created_ms DESC LIMIT 1').fetchone()
                    body = (f'这是一封 terryp57 雪球动态监控测试邮件。\n收件人：{RECIPIENT}\n'
                            f'北京时间每天 06:00–23:00 每 5 分钟检查一次（23:00 最后一轮），仅发送过去 12 小时新增且未发消息。\n'
                            f'已保留 {n} 条去重记录，不补发旧帖。\n'
                            '采集使用公开访客会话，无需保持 Chrome 打开。\n'
                            '范围为主页全部帖子（含转发及主页中的回复型帖子），不包含独立回复标签的所有评论。\n'
                            f'测试时间：{stamp()}（北京时间）。\n\n以下是最近一条已有动态示例，不是新增提醒：')
                    if row:
                        body += render_posts([json.loads(row[0])])
                    send_mail('[雪球监控测试] terryp57 · 每5分钟检查', body, f'test:{time.time_ns()}')
                    with db:
                        put_meta(db, 'test_mail_accepted_ms', int(time.time() * 1000))
                    logging.info('test_mail_smtp_accepted')
                    print('test_mail_smtp_accepted')
                else:
                    print(json.dumps(status(db), ensure_ascii=False, indent=2))
                return 0
            except Exception as e:
                error = str(e) if isinstance(e, MonitorError) else type(e).__name__
                logging.error('run_failed code=%s', error)
                print(f'failed: {error}')
                return 1
            finally:
                write_status(db)


if __name__ == '__main__':
    raise SystemExit(main())
