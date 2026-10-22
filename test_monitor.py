import json
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
        self.assertIn('posts:2,3', called[0][2])

    def test_unseen_recent_post_older_than_watermark_is_not_lost(self):
        m.ingest(self.db, [post(1, 1000)], 2000)
        m.ingest(self.db, [post(3, 4000)], 5000)
        self.assertEqual(m.ingest(self.db, [post(2, 3000)], 6000)['new'], 1)


if __name__ == '__main__':
    unittest.main()
