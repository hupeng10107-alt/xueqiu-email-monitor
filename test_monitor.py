import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import monitor as m


def post(sid, ms):
    return {'id': str(sid), 'created_ms': ms, 'title': '', 'text': '公开发言',
            'url': f'https://xueqiu.com/{m.USER_ID}/{sid}', 'truncated': False}


class MonitorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = m.init_db(Path(self.tmp.name) / 'state.sqlite3')

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def test_baseline_excludes_history_and_repeat_is_silent(self):
        old = post(1, 1000)
        self.assertTrue(m.ingest(self.db, [old], 2000)['baseline'])
        called = []
        self.assertEqual(m.deliver(self.db, lambda *a: called.append(a), now_ms=2000), 0)
        self.assertEqual(m.ingest(self.db, [old, post(2, 1500)], 3000)['new'], 0)
        self.assertEqual(called, [])

    def test_new_posts_send_once_including_equal_timestamp(self):
        m.ingest(self.db, [post(1, 1000)], 2000)
        new = post(2, 3000)
        self.assertEqual(m.ingest(self.db, [new], 4000)['new'], 1)
        self.assertEqual(m.ingest(self.db, [new, post(3, 3000)], 4500)['new'], 1)
        called = []
        sender = lambda *a: called.append(a)
        self.assertEqual(m.deliver(self.db, sender, now_ms=4500), 2)
        m.ingest(self.db, [new, post(3, 3000)], 5000)
        self.assertEqual(m.deliver(self.db, sender, now_ms=5000), 0)
        self.assertEqual(len(called), 1)

    def test_failed_mail_preserves_outbox_until_success(self):
        m.ingest(self.db, [post(1, 1000)], 2000)
        m.ingest(self.db, [post(2, 3000)], 4000)
        def fail(*args):
            raise m.MonitorError('smtp_network_or_protocol_error')
        with self.assertRaises(m.MonitorError):
            m.deliver(self.db, fail, now_ms=4000)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM posts WHERE delivery='pending'").fetchone()[0], 1)
        self.assertEqual(m.deliver(self.db, lambda *a: None, now_ms=5000), 1)
        self.assertEqual(m.deliver(self.db, lambda *a: self.fail('duplicate'), now_ms=6000), 0)

    def test_pagination_ignores_pin_and_catches_multiple_pages(self):
        def raw(sid, ms, mark=0):
            return {'id': sid, 'created_at': ms, 'user_id': m.USER_ID, 'mark': mark, 'text': '测试'}
        cutoff = 1700000000000
        pages = [
            {'page': 1, 'maxPage': 3, 'statuses': [raw(99, cutoff - 9000, 1), raw(3, cutoff + 2000)]},
            {'page': 2, 'maxPage': 3, 'statuses': [raw(2, cutoff + 1000), raw(1, cutoff - 1000)]},
        ]
        client = object.__new__(m.Xueqiu)
        calls = []
        def read(url):
            calls.append(url)
            return pages[len(calls)-1]
        client.read = read
        with patch.object(m.time, 'sleep'):
            got = client.fetch(cutoff)
        self.assertEqual([p['id'] for p in got], ['1', '2', '3'])
        self.assertEqual(len(calls), 2)

    def test_empty_feed_cannot_initialize_or_advance_state(self):
        client = object.__new__(m.Xueqiu)
        client.read = lambda url: {'statuses': [], 'page': 1}
        with self.assertRaises(m.MonitorError):
            client.fetch()
        self.assertIsNone(m.get_meta(self.db, 'initialized_ms'))

    def test_unexpected_author_rejected(self):
        with self.assertRaises(m.MonitorError):
            m.normalize({'id': 1, 'created_at': 1700000000000, 'user_id': 'other'})

    def test_beijing_window_boundaries(self):
        def at(hhmmss):
            return int(m.datetime.fromisoformat('2026-10-22T' + hhmmss + '+08:00').timestamp() * 1000)
        for value in ['06:00:00', '06:05:00', '22:55:00', '23:00:59']:
            self.assertTrue(m.in_active_window(at(value)), value)
        for value in ['05:59:59', '23:01:00', '00:00:00']:
            self.assertFalse(m.in_active_window(at(value)), value)

    def test_outside_window_never_fetches(self):
        now = m.datetime.fromisoformat('2026-10-22T05:55:00+08:00').timestamp()
        with patch.object(m.time, 'time', return_value=now), patch.object(m, 'Xueqiu') as source:
            m.check(self.db)
            source.assert_not_called()

    def test_twelve_hour_window_and_expired_outbox(self):
        now = int(m.datetime.fromisoformat('2026-10-22T18:00:00+08:00').timestamp() * 1000)
        m.ingest(self.db, [], now - 24 * 3600_000)
        old = post(1, now - m.LOOKBACK_MS - 1)
        edge = post(2, now - m.LOOKBACK_MS)
        recent = post(3, now - 300_000)
        self.assertEqual(m.ingest(self.db, [old, edge, recent], now)['new'], 2)
        # A previously queued record expires before a delayed retry.
        self.db.execute("UPDATE posts SET delivery='pending' WHERE id='1'")
        self.db.commit()
        called = []
        self.assertEqual(m.deliver(self.db, lambda *a: called.append(a), now_ms=now), 2)
        self.assertEqual(self.db.execute("SELECT delivery FROM posts WHERE id='1'").fetchone()[0], 'expired')
        self.assertIn(f'posts:{m.USER_ID}:2,{m.USER_ID}:3', called[0][2])

    def test_unseen_recent_post_older_than_watermark_is_not_lost(self):
        m.ingest(self.db, [post(1, 1000)], 2000)
        m.ingest(self.db, [post(3, 4000)], 5000)
        self.assertEqual(m.ingest(self.db, [post(2, 3000)], 6000)['new'], 1)

    def test_newly_pinned_post_is_included(self):
        cutoff = 1700000000000
        client = object.__new__(m.Xueqiu)
        client.read = lambda url: {'page':1, 'maxPage':1, 'statuses':[
            {'id':2,'created_at':cutoff+1000,'user_id':m.USER_ID,'mark':1,'text':'新置顶'},
            {'id':1,'created_at':cutoff-1000,'user_id':m.USER_ID,'mark':0,'text':'旧帖'}]}
        self.assertEqual([p['id'] for p in client.fetch(cutoff)], ['1','2'])


class MultiSourceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = m.init_db(Path(self.tmp.name)/'state.sqlite3')
        self.now = int(m.datetime.fromisoformat('2026-10-22T18:00:00+08:00').timestamp()*1000)
        for t in m.TARGETS:
            m.ingest(self.db,[],self.now-600_000,t['id'],t['name'])

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def source_post(self,t,sid=2):
        return dict(post(sid,self.now-1000),user_id=t['id'],screen_name=t['name'],
                    url=f"https://xueqiu.com/{t['id']}/{sid}")

    def test_four_sources_same_id_one_email_four_groups(self):
        for t in m.TARGETS:
            m.ingest(self.db,[self.source_post(t)],self.now,t['id'],t['name'])
        calls=[]
        self.assertEqual(m.deliver(self.db,lambda *a:calls.append(a),now_ms=self.now),4)
        self.assertEqual(len(calls),1)
        for t in m.TARGETS:
            self.assertIn(t['name']+'（'+t['id']+'）',calls[0][1])
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM posts WHERE delivery='sent'").fetchone()[0],4)
        self.assertEqual(m.deliver(self.db,lambda *a:self.fail('duplicate'),now_ms=self.now),0)

    def test_quiet_sources_are_present_in_single_updated_digest(self):
        t=m.TARGETS[0]
        m.ingest(self.db,[self.source_post(t)],self.now,t['id'],t['name'])
        calls=[]
        m.deliver(self.db,lambda *a:calls.append(a),now_ms=self.now)
        self.assertEqual(len(calls),1)
        self.assertEqual(calls[0][1].count('本轮无新增'),3)

    def test_multi_source_mail_failure_preserves_every_pending_record(self):
        for t in m.TARGETS:
            m.ingest(self.db,[self.source_post(t)],self.now,t['id'],t['name'])
        def fail(*a): raise m.MonitorError('smtp_network_or_protocol_error')
        with self.assertRaises(m.MonitorError):
            m.deliver(self.db,fail,now_ms=self.now)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM posts WHERE delivery='pending'").fetchone()[0],4)
        calls=[]
        m.deliver(self.db,lambda *a:calls.append(a),now_ms=self.now+1000)
        self.assertEqual(len(calls),1)

    def test_failed_source_does_not_stop_other_sources_or_add_an_email(self):
        failed=m.TARGETS[1]['id']
        outer=self
        class Source:
            def __init__(self,uid,name): self.uid=uid
            def fetch(self,cutoff):
                if self.uid==failed: raise m.MonitorError('xueqiu_http_400')
                t=next(t for t in m.TARGETS if t['id']==self.uid)
                return [outer.source_post(t)]
        calls=[]
        with self.assertLogs(level='ERROR'),patch.object(m.time,'time',return_value=self.now/1000),patch.object(m,'Xueqiu',Source),patch.object(m,'send_mail',side_effect=lambda *a:calls.append(a)):
            with self.assertRaises(m.MonitorError): m.check(self.db)
        self.assertEqual(len(calls),1)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM posts WHERE delivery='sent'").fetchone()[0],3)
        self.assertIn('获取状态：异常',calls[0][1])
        self.assertEqual(m.source_meta(self.db,failed,'fetch_failures'),'1')

    def test_multiple_alerts_and_updates_share_one_email(self):
        with self.db:
            for t in m.TARGETS[:2]:
                m.put_source_meta(self.db,t['id'],'last_error','xueqiu_http_400')
                m.put_source_meta(self.db,t['id'],'fetch_failures',3)
        t=m.TARGETS[3]
        m.ingest(self.db,[self.source_post(t)],self.now,t['id'],t['name'])
        calls=[]
        m.deliver(self.db,lambda *a:calls.append(a),now_ms=self.now)
        self.assertEqual(len(calls),1)
        self.assertEqual(calls[0][1].count('获取状态：异常'),2)
        m.deliver(self.db,lambda *a:self.fail('duplicate alert'),now_ms=self.now+1000)

    def test_configuration_confirmation_is_sent_once_with_all_four_sources(self):
        calls=[]
        m.deliver(self.db,lambda *a:calls.append(a),now_ms=self.now,confirm=True)
        m.deliver(self.db,lambda *a:calls.append(a),now_ms=self.now+1000,confirm=True)
        self.assertEqual(len(calls),1)
        self.assertEqual(calls[0][1].count('本轮无新增'),4)
        self.assertEqual(m.get_meta(self.db,'confirmation_revision'),m.CONFIRMATION_REVISION)

    def test_new_source_initialization_does_not_reset_existing_source(self):
        uid=m.TARGETS[0]['id']
        with self.db:
            self.db.execute('DELETE FROM meta WHERE key LIKE ?',('source:'+uid+':%',))
        existing=m.source_meta(self.db,m.USER_ID,'initialized_ms')
        self.assertTrue(m.ingest(self.db,[self.source_post(m.TARGETS[0])],self.now,uid)['baseline'])
        self.assertEqual(m.source_meta(self.db,m.USER_ID,'initialized_ms'),existing)
        self.assertEqual(m.deliver(self.db,lambda *a:self.fail('old post'),now_ms=self.now),0)

    def test_legacy_state_migration_preserves_sent_posts_and_activation(self):
        path=Path(self.tmp.name)/'legacy.sqlite3'
        with sqlite3.connect(path) as db:
            db.executescript('''CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
              CREATE TABLE posts(id TEXT PRIMARY KEY,created_ms INTEGER NOT NULL,payload TEXT NOT NULL,
              delivery TEXT NOT NULL,discovered_ms INTEGER NOT NULL,sent_ms INTEGER);''')
            db.execute('INSERT INTO meta VALUES (?,?)',('initialized_ms',str(self.now-600_000)))
            db.execute('INSERT INTO posts VALUES (?,?,?,?,?,?)',('2',self.now-1000,json.dumps(post(2,self.now-1000)),'sent',self.now-1000,self.now-500))
        db.close()
        migrated=m.init_db(path)
        try:
            self.assertEqual(m.source_meta(migrated,m.USER_ID,'initialized_ms'),str(self.now-600_000))
            self.assertEqual(migrated.execute('SELECT user_id,delivery FROM posts').fetchone()[:],(m.USER_ID,'sent'))
            self.assertEqual(m.ingest(migrated,[post(2,self.now-1000)],self.now)['new'],0)
            self.assertEqual(m.deliver(migrated,lambda *a:self.fail('legacy duplicate'),now_ms=self.now),0)
            self.assertEqual(migrated.execute('PRAGMA integrity_check').fetchone()[0],'ok')
        finally: migrated.close()


if __name__ == '__main__':
    unittest.main()
