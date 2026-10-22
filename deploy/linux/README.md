# Linux server deployment

These units target Alibaba Cloud Linux 3 with the vendor's `python3.11` package installed alongside the system Python. Keep the system Python unchanged. The monitor uses only the Python standard library and IANA timezone data.

Deploy the repository to `/opt/xueqiu-monitor`, owned by root. Create the system account `xueqiu-monitor`, with no interactive login, and a private `/var/lib/xueqiu-monitor` owned by that account. Install the units into `/etc/systemd/system`.

The root-owned `/etc/xueqiu-monitor.env` (mode 0600) must contain `MONITOR_RECIPIENT`, `SMTP_USERNAME`, and `SMTP_PASSWORD`. Transfer these values through an authenticated encrypted connection. Never put credentials in this repository, shell history, or command-line arguments.

Before cutover, probe all sources and SMTP port 465 from the actual server. Stop the previous scheduler and wait for any in-flight job. Export its newest SQLite state, validate integrity and all four initialization timestamps, and install it as `/var/lib/xueqiu-monitor/state.sqlite3` (owner `xueqiu-monitor`, mode 0600). Never substitute an obsolete migration seed or start with an empty database. The service refuses missing, corrupt, or incomplete state.

Validate the calendar on the target system:

```sh
systemd-analyze calendar '*-*-* 06..22:00/5:00 Asia/Shanghai'
systemd-analyze calendar '*-*-* 23:00:00 Asia/Shanghai'
systemd-analyze verify /etc/systemd/system/xueqiu-monitor.service /etc/systemd/system/xueqiu-monitor.timer
systemctl daemon-reload
systemctl start xueqiu-monitor.service
systemctl enable --now xueqiu-monitor.timer
```

The calendar has 205 daily slots, including 23:00. Persistent timers catch up once after downtime; the Python program still applies its actual-time window and twelve-hour expiry. A service already running is not started twice; a four-minute timeout bounds stalled checks. Source failures, source backoff, network failures, or server downtime can delay delivery.

Verify two actual timer activations and successful source-fetch timestamps before declaring the migration complete. A manual run alone does not verify the scheduler. Keep the previous scheduler disabled while this timer is enabled.

```sh
systemctl list-timers xueqiu-monitor.timer --all
systemctl status xueqiu-monitor.service --no-pager
journalctl -u xueqiu-monitor.service -n 30 --no-pager
tail -n 30 /var/lib/xueqiu-monitor/monitor.log
```

`status.json`, the database, and rotated logs stay in the private state directory. Back up SQLite using its backup API rather than copying an active WAL database. Before rollback, stop this timer, wait for the service, export the newest state, and restore it to the previous platform; do not activate two senders with different databases.
