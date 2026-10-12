"""Reproducible synthetic workload; includes both joins and manifest hashing."""
import argparse
import json
import platform
import random
import sqlite3
import statistics
import time
from pathlib import Path

from featurefence import Event, Query, Store, reference


def run(size=100_000, query_count=10_000):
    rng = random.Random(42)
    events = [Event(str(i), f'u{i%1000}', f'f{(i//1000)%3}', i, i+rng.randrange(5000), rng.random()) for i in range(size)]
    queries = [Query(str(i), f'u{rng.randrange(1000)}', rng.randrange(size+5000)) for i in range(query_count)]
    features = {f'f{i}': 20_000 for i in range(3)}
    timings = []
    with Store() as store:
        start = time.perf_counter()
        store.ingest(events)
        ingest = time.perf_counter()-start
        # Check the first 100 queries against an independent exhaustive implementation.
        start = time.perf_counter()
        oracle = reference(events, queries[:100], features)
        oracle_seconds = time.perf_counter()-start
        for _ in range(3):
            start = time.perf_counter()
            result = store.build(queries, features)
            timings.append(time.perf_counter()-start)
        actual = [{k: v['id'] if v else None for k, v in r['provenance'].items()} for r in result['rows'][:100]]
        if actual != oracle:
            raise AssertionError('indexed join differs from exhaustive oracle')
        plan = [row[3] for row in store.db.execute('EXPLAIN QUERY PLAN SELECT * FROM events WHERE entity=? AND feature=? AND event_time BETWEEN ? AND ? AND available_at<=? ORDER BY event_time DESC, available_at DESC LIMIT 1', ('u1','f0',0,size,size))]
    return {'python': platform.python_version(), 'sqlite': sqlite3.sqlite_version,
            'platform': platform.platform(), 'seed': 42, 'synthetic': True,
            'events': size, 'queries': query_count, 'features': 3, 'ingest_seconds': ingest,
            'build_seconds_three_runs': timings, 'median_build_seconds': statistics.median(timings),
            'feature_cells_per_second': query_count*3/statistics.median(timings),
            'oracle_queries_verified': min(100, query_count), 'oracle_seconds': oracle_seconds,
            'query_plan': plan, 'summary': result['summary'],
            'events_sha256': result['manifest']['events_sha256'],
            'scope': 'in-memory SQLite, warm repeated builds, safe + hindsight joins + full log hash; excludes input generation and JSON serialization'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    report = json.dumps(run(), indent=2)+'\n'
    if args.output:
        args.output.write_text(report)
    print(report, end='')
