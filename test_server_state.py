import importlib.util
from pathlib import Path
import sqlite3
import tempfile
import unittest

import monitor

spec = importlib.util.spec_from_file_location('validate_state', Path(__file__).parent / 'deploy/linux/validate_state.py')
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)


class ServerStateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / 'state.sqlite3'

    def tearDown(self):
        self.tmp.cleanup()

    def initialized(self):
        with monitor.init_db(self.path) as db:
            for target in monitor.TARGETS:
                monitor.ingest(db, [], 1700000000000, target['id'], target['name'])
        db.close()

    def test_missing_history_is_not_recreated(self):
        with self.assertRaisesRegex(ValueError, 'state_missing'):
            guard.validate(self.path, monitor.TARGETS)
        self.assertFalse(self.path.exists())

    def test_all_four_source_histories_are_required(self):
        self.initialized()
        self.assertEqual(guard.validate(self.path, monitor.TARGETS), {})
        with sqlite3.connect(self.path) as db:
            db.execute('DELETE FROM meta WHERE key=?', ('source:' + monitor.TARGETS[-1]['id'] + ':initialized_ms',))
        with self.assertRaisesRegex(ValueError, 'source_baseline_missing'):
            guard.validate(self.path, monitor.TARGETS)

    def test_stale_unmigrated_seed_is_rejected(self):
        self.initialized()
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE meta SET value='1' WHERE key='schema_version'")
        with self.assertRaisesRegex(ValueError, 'state_schema_not_migrated'):
            guard.validate(self.path, monitor.TARGETS)

    def test_corrupt_history_fails_closed(self):
        self.path.write_bytes(b'not a database')
        with self.assertRaises(sqlite3.DatabaseError):
            guard.validate(self.path, monitor.TARGETS)


if __name__ == '__main__':
    unittest.main()
