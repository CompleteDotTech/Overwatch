# Overwatch

Overwatch is a local browser service for cloud resources and training workloads.
It starts with a complete SkyPilot inventory so missing telemetry can never hide
a running machine, then adds training context when W&B, CloudWatch, durable
storage, or Zymtrace data is available.

## Current capabilities

- Shows every SkyPilot managed job from the unbounded all-user queue.
- Shows standalone clusters and development nodes without requiring a W&B run.
- Adds W&B project/run status, progress, EMA throughput, cost, retry, and ETA data
  to matching training jobs.
- Streams ANSI-colored CloudWatch logs in a full-screen browser drawer.
- Links to SkyPilot, W&B, Zymtrace, and cloud storage.
- Refreshes data periodically and reloads the service when Python, templates,
  JavaScript, or CSS changes.

The internal report is resource-oriented rather than run-oriented. SkyPilot is
currently the first cloud inventory provider, and Flow/W&B is the first optional
training enrichment provider. Additional cloud accounts, schedulers, and training
systems can be added without changing the rule that inventory determines which
resources appear.

## Install and run

Install the repository as an editable global uv tool:

```sh
uv tool install --reinstall --editable /Users/erik/code/Overwatch \
  --overrides /Users/erik/code/Overwatch/overrides.txt
overwatch
```

Open <http://127.0.0.1:8765>. Chrome opens automatically unless `--no-open` is
passed. Auto-reload is enabled unless `--no-reload` is passed.

Useful options:

```sh
overwatch --help
overwatch --port 8877 --no-open
overwatch --no-log-enrichment
```

## Optional macOS service

No LaunchAgent is installed automatically. To keep Overwatch running at login,
create `~/Library/LaunchAgents/ai.typesafe.overwatch.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>ai.typesafe.overwatch</string>
  <key>ProgramArguments</key>
  <array>
    <string>/Users/erik/.local/bin/overwatch</string>
    <string>--no-open</string>
  </array>
  <key>WorkingDirectory</key>
  <string>/Users/erik/code/Overwatch</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ThrottleInterval</key>
  <integer>5</integer>
  <key>StandardOutPath</key>
  <string>/tmp/overwatch.log</string>
  <key>StandardErrorPath</key>
  <string>/tmp/overwatch.error.log</string>
</dict>
</plist>
```

Load it with:

```sh
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/ai.typesafe.overwatch.plist
launchctl kickstart -k gui/$(id -u)/ai.typesafe.overwatch
```

Inspect or remove it with:

```sh
launchctl print gui/$(id -u)/ai.typesafe.overwatch
tail -f /tmp/overwatch.log /tmp/overwatch.error.log
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/ai.typesafe.overwatch.plist
```
