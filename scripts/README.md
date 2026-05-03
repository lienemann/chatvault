# scripts/

Operational helpers for running chatvault on a daily cadence.

## `daily.sh`

A self-contained shell script that finds the latest WA backup, runs the
chatvault extract, syncs contacts (if `termux-contact-list` is available),
and triggers a one-off media-mirror snapshot. Logs to
`$XDG_STATE_HOME/chatvault/daily.log`.

Cron-friendly. Suggested cadence: hourly.

```cron
0 * * * *  $HOME/chatvault/scripts/daily.sh
```

Override the source directory with `WA_DB_DIR=/some/path`.

## `service/chatvault-mirror/`

A [termux-services](https://wiki.termux.com/wiki/Termux-services) unit
running the media mirror daemon. Install:

```sh
pkg install termux-services inotify-tools
mkdir -p ~/.config/sv
cp -r service/chatvault-mirror ~/.config/sv/
sv-enable chatvault-mirror
sv up chatvault-mirror
```

Logs land in `$XDG_STATE_HOME/chatvault/mirror/` (rotated by `svlogd`).
