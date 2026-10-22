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
TARGETS_PATH = ROOT / 'targets.json'
TARGETS = json.loads(TARGETS_PATH.read_text()) if TARGETS_PATH.exists() else [{'id': USER_ID, 'name': NAME}]
TARGET_NAMES = {str(t['id']): t['name'] for t in TARGETS}
CONFIRMATION_REVISION = 'four-sources-v1'


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
    db.execute('BEGIN IMMEDIATE')
    db.execute('''
      CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    ''')
    columns = {r[1] for r in db.execute('PRAGMA table_info(posts)')}
    legacy = bool(columns and 'user_id' not in columns)
    if legacy:
        db.execute('ALTER TABLE posts RENAME TO posts_v1')
    db.execute('''
      CREATE TABLE IF NOT EXISTS posts (
        id TEXT NOT NULL, created_ms INTEGER NOT NULL, payload TEXT NOT NULL,
        delivery TEXT NOT NULL, discovered_ms INTEGER NOT NULL, sent_ms INTEGER,
        user_id TEXT NOT NULL, PRIMARY KEY(user_id,id)
      );
    ''')
    if legacy:
        db.execute('''INSERT INTO posts(id,created_ms,payload,delivery,discovered_ms,sent_ms,user_id)
                      SELECT id,created_ms,payload,delivery,discovered_ms,sent_ms,? FROM posts_v1''', (USER_ID,))
        db.execute('DROP TABLE posts_v1')
    if get_meta(db, 'schema_version') != '2':
        for key in ['initialized_ms','watermark_ms','last_fetch_ms','fetch_failures','backoff_until_ms','last_error','last_alert_ms']:
            value = get_meta(db, key)
            if value is not None:
                put_meta(db, f'source:{USER_ID}:{key}', value)
        put_meta(db, 'schema_version', 2)
    db.commit()
    return db


def get_meta(db, key, default=None):
    row = db.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()
    return row[0] if row else default


def put_meta(db, key, value):
    db.execute('INSERT OR REPLACE INTO meta VALUES (?,?)', (key, str(value)))


def source_meta(db, uid, key, default=None):
    return get_meta(db, f'source:{uid}:{key}', default)


def put_source_meta(db, uid, key, value):
    put_meta(db, f'source:{uid}:{key}', value)


def normalize(s, user_id=USER_ID, name=NAME):
    if str(s.get('user_id', s.get('user', {}).get('id', ''))) != user_id:
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
            'text': text, 'url': f'https://xueqiu.com/{user_id}/{sid}',
            'user_id': user_id, 'screen_name': name,
            'truncated': bool(s.get('truncated'))}


class Xueqiu:
    user_id = USER_ID
    name = NAME
    profile = PROFILE

    def __init__(self, user_id=USER_ID, name=NAME):
        self.user_id = str(user_id)
        self.name = name
        self.profile = f'https://www.xueqiu.com/u/{self.user_id}'
        # Visitor cookies stay in memory; never read the user's browser cookie store.
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
        self.opener.addheaders = [('User-Agent', 'Mozilla/5.0'), ('Referer', self.profile)]
        self.read(self.profile, expect_json=False)

    def read(self, url, expect_json=True):
        try:
            with self.opener.open(url, timeout=25) as r:
                if urllib.parse.urlparse(r.url).hostname not in ('www.xueqiu.com', 'xueqiu.com'):
                    raise MonitorError('unexpected_source_redirect')
                body = r.read(8_000_000)
            if not expect_json:
                if f'"id":{self.user_id}' not in body.decode('utf-8', 'replace'):
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
            params = urllib.parse.urlencode({'user_id': self.user_id, 'page': page, 'type': 10, 'count': 20})
            data = self.read('https://www.xueqiu.com/v4/statuses/user_timeline.json?' + params)
            if int(data.get('page', page)) != page:
                raise MonitorError('unexpected_page')
            posts = []
            for raw in data['statuses']:
                p = normalize(raw, self.user_id, self.name)
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


def ingest(db, posts, now_ms, user_id=USER_ID, name=None):
    initialized = source_meta(db, user_id, 'initialized_ms')
    baseline = initialized is None
    watermark = int(source_meta(db, user_id, 'watermark_ms', 0))
    new_count = 0
    with db:
        for p in posts:
            if str(p.get('user_id', user_id)) != user_id:
                raise MonitorError('ingest_source_mismatch')
            p = dict(p, user_id=user_id, screen_name=name or TARGET_NAMES.get(user_id, user_id))
            # Keep original baseline posts for audit; never send historical backfill.
            cutoff = max(int(initialized or now_ms), now_ms - LOOKBACK_MS)
            eligible = not baseline and cutoff <= p['created_ms'] <= now_ms
            state = 'pending' if eligible else 'baseline'
            cur = db.execute('INSERT OR IGNORE INTO posts VALUES (?,?,?,?,?,NULL,?)',
                             (p['id'], p['created_ms'], json.dumps(p, ensure_ascii=False), state, now_ms, user_id))
            new_count += int(eligible and cur.rowcount == 1)
        if baseline:
            put_source_meta(db, user_id, 'initialized_ms', now_ms)
            put_source_meta(db, user_id, 'watermark_ms', now_ms)
            if get_meta(db, 'initialized_ms') is None:
                put_meta(db, 'initialized_ms', now_ms)
        elif posts:
            put_source_meta(db, user_id, 'watermark_ms', max(watermark, max(p['created_ms'] for p in posts)))
        put_source_meta(db, user_id, 'last_fetch_ms', now_ms)
        put_source_meta(db, user_id, 'fetch_failures', 0)
        put_source_meta(db, user_id, 'backoff_until_ms', 0)
        put_source_meta(db, user_id, 'last_error', '')
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
        blocks.append(f"{p.get('screen_name', NAME)} · {stamp(p['created_ms'])}\n{p['title']}\n{p['text']}\n"
                      + ('[列表可能截断，请打开原帖查看全文]\n' if p.get('truncated') else '')
                      + f"原帖：{p['url']}")
    return '\n\n' + ('\n\n' + '—' * 30 + '\n\n').join(blocks)


def deliver(db, sender=None, now_ms=None, confirm=False):
    sender = sender or send_mail
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    if not in_active_window(now_ms):
        return 0
    with db:
        db.execute("UPDATE posts SET delivery='expired' WHERE delivery='pending' AND created_ms<?",
                   (now_ms - LOOKBACK_MS,))
    rows = db.execute("SELECT user_id,id,payload FROM posts WHERE delivery='pending' ORDER BY created_ms,user_id,id").fetchall()
    # A digest must always contain at least one eligible, unsent post.
    # Source errors and configuration changes alone must never send email.
    if not rows:
        return 0
    confirm = confirm and get_meta(db, 'confirmation_revision') != CONFIRMATION_REVISION
    grouped = {t['id']: [] for t in TARGETS}
    for row in rows:
        if row['user_id'] not in grouped:
            raise MonitorError('unconfigured_pending_source')
        p = dict(json.loads(row['payload']), user_id=row['user_id'],
                 screen_name=TARGET_NAMES[row['user_id']])
        grouped[row['user_id']].append(p)
    body = (f'雪球四源合并监控 · {stamp(now_ms)}（北京时间）\n'
            '每日 06:00–23:00 每 5 分钟检查，仅推送过去 12 小时内未发送的新动态。\n'
            f'本封合计 {len(rows)} 条；四个跟踪来源按下列分组列出。\n')
    if confirm:
        body += '四个来源已合并到同一监控；新来源在首次成功检查时登记现有帖子，不补发旧帖。\n'
    for t in TARGETS:
        uid = t['id']
        error = source_meta(db, uid, 'last_error', '')
        last = source_meta(db, uid, 'last_fetch_ms')
        body += f"\n{'='*30}\n{t['name']}（{uid}）\n来源：https://xueqiu.com/u/{uid}\n"
        if error:
            body += f'获取状态：异常（{error}），已有待发记录继续保留；其他来源照常检查。\n'
        elif last:
            body += f'最近成功检查：{stamp(int(last))}\n'
        else:
            body += '获取状态：等待首次成功检查。\n'
        if grouped[uid]:
            body += render_posts(grouped[uid]) + '\n'
        else:
            body += ('本轮暂无可推送记录，是否有新增尚待获取恢复。\n' if error else '本轮无新增。\n')
    subject = (f'[雪球监控已更新] 四个来源合并推送 · {len(rows)} 条新动态' if confirm else
               f'[雪球动态汇总] 四个来源 · {len(rows)} 条新发言')
    key = 'posts:' + ','.join(f"{r['user_id']}:{r['id']}" for r in rows)
    sender(subject, body, key)
    now = int(time.time() * 1000)
    with db:
        db.executemany("UPDATE posts SET delivery='sent',sent_ms=? WHERE user_id=? AND id=?",
                       [(now,r['user_id'],r['id']) for r in rows])
        put_meta(db, 'last_delivery_ms', now)
        put_meta(db, 'delivered_message_count', int(get_meta(db,'delivered_message_count',0))+1)
        if confirm:
            put_meta(db,'confirmation_revision',CONFIRMATION_REVISION)
            put_meta(db,'configuration_mail_accepted_ms',now)
    logging.info('digest_smtp_accepted sources=%s new_posts=%s confirmation=%s',len(TARGETS),len(rows),confirm)
    return len(rows)


def status(db):
    meta = dict(db.execute('SELECT key,value FROM meta'))
    counts = dict(db.execute('SELECT delivery,COUNT(*) FROM posts GROUP BY delivery'))
    sources = []
    for t in TARGETS:
        uid = t['id']
        sources.append(dict(t, last_fetch_ms=source_meta(db,uid,'last_fetch_ms'),
                            initialized_ms=source_meta(db,uid,'initialized_ms'),
                            last_error=source_meta(db,uid,'last_error',''),
                            counts=dict(db.execute('SELECT delivery,COUNT(*) FROM posts WHERE user_id=? GROUP BY delivery',(uid,)))))
    return {'user': NAME, 'user_id': USER_ID, 'recipient': RECIPIENT, 'interval_seconds': 300,
            'sources': sources, 'source_count':len(TARGETS), 'mail_mode':'one_combined_digest_per_run',
            'timezone': 'Asia/Shanghai', 'active_window': '06:00-23:00 (last minute 23:00)',
            'lookback_hours': 12,
            'meta': meta, 'counts': counts, 'reported_at': stamp()}


def write_status(db):
    tmp = RUNTIME / 'status.tmp'
    tmp.write_text(json.dumps(status(db), ensure_ascii=False, indent=2))
    tmp.replace(RUNTIME / 'status.json')


def check(db, confirm=False):
    now = int(time.time() * 1000)
    if not in_active_window(now):
        with db:
            put_meta(db, 'last_window_skip_ms', now)
        logging.info('outside_active_window')
        return
    errors = []
    for target in TARGETS:
        uid, name = target['id'], target['name']
        now = int(time.time()*1000)
        if not in_active_window(now):
            break
        if now < int(source_meta(db,uid,'backoff_until_ms',0)):
            logging.info('source_backoff user_id=%s',uid)
            continue
        try:
            posts = Xueqiu(uid,name).fetch(now-LOOKBACK_MS)
            fetched_ms = int(time.time()*1000)
            result = ingest(db,posts,fetched_ms,uid,name)
            logging.info('fetch_ok user_id=%s %s',uid,json.dumps(result))
        except Exception as e:
            error = str(e) if isinstance(e,MonitorError) else type(e).__name__
            errors.append(f'{uid}:{error}')
            with db:
                failures = int(source_meta(db,uid,'fetch_failures',0))+1
                put_source_meta(db,uid,'fetch_failures',failures)
                put_source_meta(db,uid,'last_error',error)
                if failures>=3:
                    put_source_meta(db,uid,'backoff_until_ms',now+30*60_000)
            logging.error('fetch_failed user_id=%s code=%s failures=%s',uid,error,failures)
    with db:
        latest = [int(source_meta(db,t['id'],'last_fetch_ms',0)) for t in TARGETS]
        put_meta(db,'last_fetch_ms',max(latest))
        put_meta(db,'last_error',';'.join(f"{t['id']}:{source_meta(db,t['id'],'last_error')}"
                                         for t in TARGETS if source_meta(db,t['id'],'last_error','')))
        put_meta(db,'fetch_failures',max(int(source_meta(db,t['id'],'fetch_failures',0)) for t in TARGETS))
    try:
        sent = deliver(db,confirm=confirm)
        logging.info('delivery_ok sent=%s', sent)
        with db:
            put_meta(db, 'delivery_error', '')
    except Exception as e:
        error = str(e) if isinstance(e, MonitorError) else type(e).__name__
        with db:
            put_meta(db, 'delivery_error', error)
        logging.error('delivery_failed code=%s pending_preserved=true', error)
        raise MonitorError(error) from None
    if errors:
        raise MonitorError(';'.join(errors))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['check','check-and-confirm','status','test-mail'])
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
                if args.action in ('check','check-and-confirm'):
                    check(db,confirm=args.action=='check-and-confirm')
                elif args.action == 'test-mail':
                    # Retain the old command as a safe no-op for existing callers.
                    logging.info('test_mail_disabled_new_posts_only')
                    print('test_mail_disabled_new_posts_only')
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
