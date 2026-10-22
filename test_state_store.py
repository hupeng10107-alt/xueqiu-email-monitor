import base64
import gzip
import io
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import zipfile

import monitor
import state_store as store


class StateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.runtime = self.root / 'runtime'
        self.database = self.runtime / 'state.sqlite3'
        self.patches = [patch.object(store, 'RUNTIME', self.runtime),
                        patch.object(store, 'DATABASE', self.database)]
        for p in self.patches:
            p.start()

    def tearDown(self):
        for p in self.patches:
            p.stop()
        self.tmp.cleanup()

    def seed(self):
        path = self.root / 'seed.sqlite3'
        db = monitor.init_db(path)
        monitor.ingest(db, [], 123456)
        db.close()
        return base64.b64encode(gzip.compress(path.read_bytes())).decode()

    def test_normal_run_missing_state_never_uses_seed(self):
        with patch.object(store, 'artifacts', return_value=[]), patch.dict(store.os.environ, {'MONITOR_SEED_STATE_GZ':self.seed()}):
            with self.assertRaisesRegex(store.StateError, 'missing_state_fail_closed'):
                store.restore()
        self.assertFalse(self.database.exists())

    def test_bootstrap_cannot_overwrite_existing_cloud_state(self):
        with patch.object(store, 'artifacts', return_value=[{'id':1}]):
            with self.assertRaisesRegex(store.StateError, 'bootstrap_refused_existing_state'):
                store.restore(seed=True)

    def test_seed_restores_initialized_state(self):
        with patch.object(store, 'artifacts', return_value=[]), patch.dict(store.os.environ, {'MONITOR_SEED_STATE_GZ':self.seed()}):
            store.restore(seed=True)
        with sqlite3.connect(self.database) as db:
            self.assertEqual(db.execute("SELECT value FROM meta WHERE key='initialized_ms'").fetchone()[0], '123456')
        db.close()

    def test_artifact_with_unexpected_paths_is_rejected(self):
        data = io.BytesIO()
        with zipfile.ZipFile(data, 'w') as z:
            z.writestr('../state.sqlite3', 'invalid')
        with patch.object(store, 'artifacts', return_value=[{'id':1}]), patch.object(store, 'api', return_value=data.getvalue()):
            with self.assertRaisesRegex(store.StateError, 'invalid_state_archive'):
                store.restore()
        self.assertFalse(self.database.exists())


if __name__ == '__main__':
    unittest.main()
