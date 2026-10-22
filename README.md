# Public timeline email monitor

A standard-library Python monitor for a Linux server or GitHub-hosted Ubuntu runners. Email addresses and SMTP credentials belong in private runtime configuration, never in source control.

The active deployment uses a Linux server with a systemd timer; the former GitHub workflow is disabled to prevent duplicate senders. See [Linux deployment and operations](deploy/linux/README.md). The GitHub setup below is retained as an alternative and must stay disabled while the server timer is active.

## Four sources, one combined email

`targets.json` tracks PaulWu (1965894836), 梁宏 (9887656769), 老鬼的逻辑 (1064583172), and terryp57 (2151206013). Each run checks all four and sends at most one email, with a section for each source. Quiet sources are explicitly marked. If all sources are quiet and no error alert is due, no email is sent. Source errors and new posts share the same email; a failed source does not block the others.

State migrates automatically from the original single-source database. Existing terryp57 delivery history is preserved. New sources establish their own baseline on their first successful check, without sending existing posts. Deduplication uses `(source user ID, post ID)`. `check-and-confirm` manually sends one configuration confirmation (combined with new posts if any); its revision is persisted so repeating it does not send another confirmation.

## Schedule and content rules

- Timezone: Asia/Shanghai (UTC+8, no daylight saving).
- Every five minutes from 06:00 through 22:55, plus one final 23:00 check.
- GitHub scheduled jobs can be delayed or dropped. The program checks actual wall-clock time again: no source request or alert outside the window, including delayed jobs after 23:00's minute.
- Inspect a rolling twelve-hour timeline; send only unseen posts published after the original monitor activation and within those twelve hours. Retried messages expire too.
- Scope: the profile's All timeline, including reposts and reply-style posts listed there. The separate Replies tab's complete comment history is outside scope.
- Posts are deduplicated by source user ID and post ID in SQLite. SMTP failures leave a durable outbox. An SMTP-accepted message followed by a crash before the database commit may be retried; deterministic Message-ID helps recipient deduplication.

## Alternative GitHub deployment configuration

Secrets:

- `MONITOR_RECIPIENT`: destination email.
- `SMTP_USERNAME`: existing 126.com SMTP account.
- `SMTP_PASSWORD`: existing SMTP authorization code.
- `MONITOR_SEED_STATE_GZ`: base64-encoded gzip SQLite backup for one-time migration.

Variable `MONITOR_ENABLED=true` enables scheduled runs. It stays false during migration validation. Manually dispatched `diagnose`, `bootstrap`, and `check` modes remain available.

`diagnose` validates public source access and sends a labeled test email without sending newly discovered posts or publishing a state snapshot. `bootstrap` migrates the seed once, refuses to replace an existing cloud snapshot, then performs a normal check. All later runs restore the newest snapshot. Missing or corrupt cloud state stops sending instead of rebuilding an empty history.

Only the newest three snapshots are retained, each with a 90-day retention limit. Snapshots contain public timeline payloads, IDs, and delivery metadata, without SMTP credentials or email addresses. State is saved even if source acquisition or SMTP fails. Prolonged inactivity beyond artifact retention requires a deliberate state recovery.

Every thirty days, the workflow updates only `ops/heartbeat.json`, avoiding GitHub's automatic schedule disablement after sixty days without repository activity. The workflow does not run on pushes or pull requests. Official actions are pinned by commit SHA. Its token is scoped to this repository.

## References

- [GitHub scheduled workflow behavior](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule)
- [GitHub Actions billing](https://docs.github.com/en/billing/concepts/product-billing/github-actions)

Standard GitHub-hosted runners in public repositories are free; do not switch this repository to private or use larger runners without reviewing billing. Automatic cleanup keeps state storage small.
