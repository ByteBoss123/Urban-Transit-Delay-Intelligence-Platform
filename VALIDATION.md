# Validation — 2026-10-28

Executed with Python 3.12.14 in a Linux workspace:

- `python -m unittest discover -s tests -v`: **5 tests passed**.
- Started `python -m transit.app local` against a temporary SQLite database.
  HTTP readiness passed after two ticks: **120 persisted events, 60 distinct
  active trips, three routes, live worker**. Stopped the process after checking.
- Python compilation passed for the supported `transit/` package.
- Compose and workflow YAML parsed successfully. Parsing is not runtime validation.

The integration tests verify simulator → database → aggregation → HTTP output,
exact replay, reopening persisted storage, invalid-record quarantine, out-of-order
input, window expiry, agency/service-date separation, and the 35% threshold.

## Not established by these checks

Docker is not installed in the local workspace. The separate GitHub Actions
[validation run](https://github.com/ByteBoss123/Urban-Transit-Delay-Intelligence-Platform/actions/runs/40737053469)
passed on implementation commit `0655ed5c0b44518ca6bb7d38aabcb5fc1c4a7d57`.
Both jobs passed: local tests and Kafka Compose configuration, image build,
startup, consumed-event/API smoke verification, and teardown. The smoke check
requires at least 60 persisted events, three routes and a flagged route.
This validates the synthetic Kafka reference path, not the experimental layers.
No cloud deployment,
live agency feed, Spark/Delta/HBase execution, ML accuracy, load benchmark,
production SLA or business-impact measurement is claimed.

Browser rendering was not verified: Playwright was available but its Chromium
binary was missing, and the browser download failed. HTTP/HTML integration
checks passed; a visual browser review remains outstanding.
