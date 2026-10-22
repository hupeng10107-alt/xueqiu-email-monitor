"""Keep the newest three durable snapshots in GitHub Actions artifacts."""
import base64
from datetime import datetime, timezone
import gzip
import io
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile

ARTIFACT_NAME = 'monitor-state'
RUNTIME = Path('runtime')
DATABASE = RUNTIME / 'state.sqlite3'


class StateError(Exception):
    pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def api(path, method='GET', payload=None, download=False):
    repo = os.environ['GITHUB_REPOSITORY']
    if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repo):
        raise StateError('invalid_repository')
    url = f'https://api.github.com/repos/{repo}/{path}'
    data = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(url, data=data, method=method, headers={
        'Authorization': 'Bearer ' + os.environ['GH_TOKEN'],
        'Accept': 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28',
        'User-Agent': 'xueqiu-email-monitor',
    })
    try:
        with urllib.request.build_opener(NoRedirect).open(request, timeout=30) as r:
            body = r.read(50_000_000)
        return json.loads(body) if body else None
    except urllib.error.HTTPError as e:
        if download and e.code == 302:
            location = e.headers.get('Location', '')
            u = urllib.parse.urlparse(location)
            if u.scheme != 'https' or not u.hostname or not u.hostname.endswith(
                    ('.blob.core.windows.net', '.githubusercontent.com')):
                raise StateError('untrusted_artifact_redirect') from None
            # Never send the GitHub token to a signed artifact download URL.
            with urllib.request.urlopen(location, timeout=30) as r:
                return r.read(50_000_000)
        if e.code == 404:
            return None
        raise StateError(f'github_http_{e.code}') from None


def artifacts():
    data = api(f'actions/artifacts?name={ARTIFACT_NAME}&per_page=100')
    return sorted([a for a in data['artifacts']
                   if not a['expired'] and a.get('workflow_run', {}).get('head_branch') == 'main'],
                  key=lambda a: a['id'], reverse=True)


def validate_database():
    with sqlite3.connect(DATABASE) as db:
        if db.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
            raise StateError('state_database_corrupt')
        if not db.execute("SELECT value FROM meta WHERE key='initialized_ms'").fetchone():
            raise StateError('state_not_initialized')
    DATABASE.chmod(0o600)


def restore(seed=False):
    RUNTIME.mkdir(exist_ok=True, mode=0o700)
    if seed:
        # This mode is allowed only for the explicit, one-time migration run.
        if artifacts():
            raise StateError('bootstrap_refused_existing_state')
        body = gzip.decompress(base64.b64decode(os.environ['MONITOR_SEED_STATE_GZ'], validate=True))
        if len(body) > 50_000_000:
            raise StateError('state_too_large')
        DATABASE.write_bytes(body)
        source = 'migration_seed'
    else:
        saved = artifacts()
        if not saved:
            raise StateError('missing_state_fail_closed')
        archive = api(f"actions/artifacts/{saved[0]['id']}/zip", download=True)
        if not isinstance(archive, bytes):
            raise StateError('state_download_failed')
        with zipfile.ZipFile(io.BytesIO(archive)) as z:
            if z.namelist() != ['state.sqlite3'] or z.infolist()[0].file_size > 50_000_000:
                raise StateError('invalid_state_archive')
            DATABASE.write_bytes(z.read('state.sqlite3'))
        source = f"artifact_{saved[0]['id']}"
    validate_database()
    print('state_restored source=' + source)


def prepare():
    validate_database()
    out = Path('artifact')
    out.mkdir(exist_ok=True)
    target = out / 'state.sqlite3'
    with sqlite3.connect(DATABASE) as db, sqlite3.connect(target) as copy:
        db.backup(copy)
        # A rolling 12-hour monitor needs no old delivered payloads. Keep 72h for audit.
        copy.execute("DELETE FROM posts WHERE delivery IN ('sent','baseline','expired') AND created_ms<?",
                     (int(time.time()*1000) - 72*3600_000,))
        copy.commit()
    target.chmod(0o600)
    print('state_prepared')


def prune():
    saved = artifacts()
    if len(saved) < 1:
        raise StateError('uploaded_state_not_found')
    for entry in saved[3:]:
        api(f"actions/artifacts/{entry['id']}", method='DELETE')
    print(f'state_retained count={min(3,len(saved))}')


def keepalive():
    path = 'contents/ops/heartbeat.json'
    current = api(path)
    now = datetime.now(timezone.utc)
    if current:
        previous = json.loads(base64.b64decode(current['content']))
        if (now - datetime.fromisoformat(previous['updated_at'])).days < 30:
            print('repository_activity_recent')
            return
    content = json.dumps({'updated_at': now.isoformat()}, indent=2) + '\n'
    payload = {'message': 'Maintain scheduled monitor activity', 'branch': 'main',
               'content': base64.b64encode(content.encode()).decode()}
    if current:
        payload['sha'] = current['sha']
    api(path, method='PUT', payload=payload)
    print('repository_activity_updated')


def main():
    os.umask(0o077)
    try:
        action = sys.argv[1]
        if action == 'restore':
            restore(seed='--seed' in sys.argv)
        elif action == 'prepare':
            prepare()
        elif action == 'prune':
            prune()
        elif action == 'keepalive':
            keepalive()
        else:
            raise StateError('unknown_state_action')
    except Exception as e:
        # API errors can include signed URLs. Print only safe codes, never exception bodies.
        print('state_error: ' + (str(e) if isinstance(e, StateError) else type(e).__name__))
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
