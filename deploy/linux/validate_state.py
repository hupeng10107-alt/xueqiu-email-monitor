"""Fail closed if an existing server's delivery history is missing or incomplete."""
import json
from contextlib import closing
from pathlib import Path
import sqlite3
import sys


def validate(path, targets):
    path = Path(path).resolve()
    if not path.is_file() or not path.stat().st_size:
        raise ValueError('state_missing')
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as db:
        if db.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
            raise ValueError('state_corrupt')
        meta = dict(db.execute('SELECT key,value FROM meta'))
        if meta.get('schema_version') != '2':
            raise ValueError('state_schema_not_migrated')
        for target in targets:
            if int(meta.get('source:' + str(target['id']) + ':initialized_ms', 0)) <= 0:
                raise ValueError('source_baseline_missing')
        return dict(db.execute('SELECT delivery,COUNT(*) FROM posts GROUP BY delivery'))


if __name__ == '__main__':
    try:
        targets = json.loads((Path(__file__).resolve().parents[2] / 'targets.json').read_text())
        result = validate(sys.argv[1], targets)
    except Exception as error:
        print('state_guard_failed: ' + (str(error) if isinstance(error, ValueError) else type(error).__name__))
        raise SystemExit(1)
    print('state_guard_ok ' + json.dumps(result, sort_keys=True))
