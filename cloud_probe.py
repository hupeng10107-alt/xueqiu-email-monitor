"""Read-only source probe, followed by an explicitly labeled SMTP test."""
import time
import monitor

now = int(time.time() * 1000)
if not monitor.in_active_window(now):
    raise SystemExit('diagnostic_outside_active_window')
try:
    posts = monitor.Xueqiu().fetch(now - monitor.LOOKBACK_MS)
    recent = [p for p in posts if now-monitor.LOOKBACK_MS <= p['created_ms'] <= now]
    print(f'cloud_source_ok fetched={len(posts)} recent_12h={len(recent)}')
except Exception as e:
    print('cloud_source_failed: ' + (str(e) if isinstance(e, monitor.MonitorError) else type(e).__name__))
    raise SystemExit(1)
