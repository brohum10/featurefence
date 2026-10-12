# FeatureFence

**Your model cannot use tomorrow's data to predict yesterday.**

FeatureFence builds historical ML feature matrices while respecting both **when an event happened** and **when its value became available**. It also shows exactly where an event-time-only join would leak future knowledge.

A dependency-free Python + SQLite project focused on ML data engineering: late arrivals, corrections, feature expiration, atomic ingestion, read-snapshot consistency, and traceable training inputs. It is not a model trainer or a full feature-store replacement.

## The bug, in one example

At time 20 you need a customer's spending feature:

| Record | Event time | Available at | Value | Usable at 20? |
|---|---:|---:|---:|---|
| Original | 10 | 12 | 80 | Yes |
| Correction of original | 10 | 19 | 85 | Yes; replaces 80 |
| Late-arriving newer event | 18 | 25 | 900 | **No** |

An event-time-only join picks **900**. FeatureFence picks **85** and reports the unavailable record as an explicit leakage witness. At time 25, the 900 record becomes eligible. This distinction matters when backfills make retrospective training data look better than information available in real use.

## Run in under a minute

Python 3.12+; **no installation or API keys required**.

```sh
python -m unittest -v
python featurefence.py example.json
python featurefence.py example.json --database local-store.sqlite --output local-result.json
python featurefence.py example.json --fail-on-leakage
```

The last command intentionally exits **2** because the sample's naive join leaks in two cells. A normal successful build exits 0; invalid input exits 1. `--fail-on-leakage` audits the unsafe alternative; it does **not** mean the safe output contains those leaked values.

Inspect [the complete sample result](example-result.json): values, chosen source records, missing cells, unsafe alternatives, and a dataset fingerprint. Replaying an identical input into the same database does not duplicate events.

## Contract

Input JSON contains `events`, `queries`, and `feature_ttl`; see [example.json](example.json).

- All times are nonnegative integer ticks in **one caller-chosen unit** (for example UTC epoch milliseconds). TTL uses the same unit. There is no implicit timezone conversion.
- An eligible record has the same entity/feature, `event_time <= query.at`, and `available_at <= query.at`.
- A finite TTL additionally requires `query.at - event_time <= TTL`. Both boundaries are inclusive; **TTL 0 means exact event time**, and `null` means unbounded history.
- Choose the newest eligible event, then its newest available revision. A late correction to an older event does not supersede a newer event.
- Values are finite scalar numbers. Missing values remain `null`, never silently zero-filled. Query IDs must be unique; query order is preserved.
- Exact duplicate IDs are idempotent; conflicting IDs or indistinguishable versions reject and roll back the entire ingestion batch.

## How it works

```text
Validated events ──atomic append──► SQLite event log
                                     │ composite entity/feature/time index
Queries + per-feature TTL ────────────┤ one consistent read snapshot
                                     ├─ safe join: event AND availability cutoffs
                                     ├─ hindsight join: event cutoff only
                                     └─ values + provenance + leakage witnesses + log hash
```

SQLite WAL supports readers during append operations. A matrix build explicitly holds one read transaction, so its values, audit, event count, and fingerprint refer to the same snapshot. SQL values are parameterized. The append-only log preserves corrections instead of overwriting history.

The index narrows each lookup by entity, feature and event-time range. Availability filtering may still scan many revisions; worst-case performance is **not** guaranteed logarithmic. Matrix construction materializes output in memory and computes a full-log hash, so the manifest adds O(N) work per build.

## Measured results

Local synthetic run on Apple Silicon, Python 3.14.2, SQLite 3.50.4:

| Workload | Result |
|---|---:|
| Stored feature events | 100,000 |
| Prediction queries × features | 10,000 × 3 |
| Median complete build, three runs | **0.670 seconds** |
| Feature cells processed per second | **44,776** |
| Unsafe cells identified in the intentionally delayed fixture | 20,291 / 30,000 |

Build timing includes safe and hindsight lookups plus full-log hashing. It excludes input generation and JSON serialization; SQLite is in-memory and repeated builds are warm. **Not production throughput, a real leakage prevalence estimate, or a comparison against Feast.**

```sh
python benchmark.py --output local-benchmark.json
```

[Raw results](benchmark-results.json) include all three timings, runtime versions, seed, fingerprint and SQLite query plan. The benchmark independently verifies the first 100 queries against an exhaustive scan; it deliberately makes no speedup claim between unequal workloads.

## Correctness evidence

**20 tests**, including 30 seeded randomized scenarios (2,250 feature cells) compared with exhaustive selection and audit logic. Coverage includes arrival boundaries, corrections, TTL 0, missing data, entity isolation, conflict rollback, replay, reopen recovery, insertion-order invariance and concurrent-append snapshot consistency. GitHub Actions runs the tests and sample on Python 3.12 and 3.14.

## Limits and next steps

- Availability timestamps are supplied by the caller: dishonest or backdated timestamps cannot be detected here. Record actual serving availability upstream.
- Only per-cell temporal leakage is checked. This does not detect target leakage inside a feature calculation, train/test entity overlap, or preprocessing fit on the test set.
- Scalar features and one entity key only; no online API, distributed ingestion, schema migration framework, deletion/tombstone semantics, authentication, or access control.
- The fingerprint identifies normalized stored events, not the original input bytes. It is an integrity aid, not a signature. Query specifications and TTLs must also be retained for reproduction.
- Recommended extensions: offline Parquet adapters, dataset lineage/versioning, and comparisons on a real delayed-arrival dataset with known serving timestamps.

## Background and authorship

[Feast's point-in-time join documentation](https://docs.feast.dev/getting-started/concepts/point-in-time-joins) explains historical retrieval and TTL. FeatureFence is an independent educational implementation, not a Feast integration or a claim of a novel algorithm. Developed with AI assistance; sample data is synthetic and all reported numbers come from the included benchmark.
