"""Point-in-time training features with explicit availability-time leakage audits."""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path
import sqlite3


def name(value, field):
    if not isinstance(value, str) or not value.strip() or len(value) > 256:
        raise ValueError(f'{field} must be a nonempty string of at most 256 characters')


def timestamp(value, field):
    if type(value) is not int or not 0 <= value <= 2**63-1:
        raise ValueError(f'{field} must be a nonnegative signed-64-bit integer')


@dataclass(frozen=True)
class Event:
    id: str
    entity: str
    feature: str
    event_time: int
    available_at: int
    value: float

    def __post_init__(self):
        for field in ('id', 'entity', 'feature'):
            name(getattr(self, field), field)
        timestamp(self.event_time, 'event_time')
        timestamp(self.available_at, 'available_at')
        if self.available_at < self.event_time:
            raise ValueError('available_at must not precede event_time')
        try:
            finite = type(self.value) in (int, float) and math.isfinite(float(self.value))
        except OverflowError:
            finite = False
        if not finite:
            raise ValueError('value must be finite numeric data, not a boolean')


@dataclass(frozen=True)
class Query:
    id: str
    entity: str
    at: int

    def __post_init__(self):
        name(self.id, 'query id')
        name(self.entity, 'entity')
        timestamp(self.at, 'prediction time')


def validate_features(features):
    if not isinstance(features, dict) or not features:
        raise ValueError('features must map feature names to TTL or null')
    for feature, ttl in features.items():
        name(feature, 'feature')
        if ttl is not None:
            timestamp(ttl, 'TTL')


class Store:
    """Append-only feature log. Each matrix build uses one SQLite read snapshot."""

    def __init__(self, path=':memory:'):
        self.db = sqlite3.connect(path, timeout=5)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY, entity TEXT NOT NULL, feature TEXT NOT NULL,
                event_time INTEGER NOT NULL, available_at INTEGER NOT NULL,
                value REAL NOT NULL,
                UNIQUE(entity, feature, event_time, available_at)
            );
            CREATE INDEX IF NOT EXISTS point_in_time
                ON events(entity, feature, event_time DESC, available_at DESC);
        ''')

    def close(self):
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def ingest(self, events):
        """Atomic batch; exact replay is a no-op, conflicting IDs/versions fail."""
        inserted = 0
        with self.db:
            for event in events:
                if not isinstance(event, Event):
                    raise ValueError('ingest accepts validated Event instances')
                # Normalize numeric representation to match SQLite REAL storage.
                values = (event.id, event.entity, event.feature, event.event_time,
                          event.available_at, float(event.value))
                existing = self.db.execute('SELECT * FROM events WHERE id=?', (event.id,)).fetchone()
                if existing:
                    if tuple(existing) != values:
                        raise ValueError(f'conflicting event id: {event.id}')
                    continue
                try:
                    self.db.execute('INSERT INTO events VALUES (?,?,?,?,?,?)', values)
                except sqlite3.IntegrityError as exc:
                    raise ValueError('ambiguous version: entity/feature/event/availability must be unique') from exc
                inserted += 1
        return inserted

    def _select(self, query, feature, ttl, known=True):
        # TTL is relative to prediction time, never wall-clock time.
        lower = max(0, query.at-ttl) if ttl is not None else 0
        sql = '''SELECT * FROM events WHERE entity=? AND feature=?
                 AND event_time BETWEEN ? AND ?'''
        params = [query.entity, feature, lower, query.at]
        if known:
            sql += ' AND available_at<=?'
            params.append(query.at)
        sql += ' ORDER BY event_time DESC, available_at DESC LIMIT 1'
        row = self.db.execute(sql, params).fetchone()
        return dict(row) if row else None

    def build(self, queries, features):
        queries = list(queries)
        validate_features(features)
        if any(not isinstance(q, Query) for q in queries):
            raise ValueError('queries must be Query instances')
        if len({q.id for q in queries}) != len(queries):
            raise ValueError('query IDs must be unique')
        rows, leaks = [], []
        with self.db:
            self.db.execute('BEGIN')
            for query in queries:
                row = {'query_id': query.id, 'entity': query.entity, 'at': query.at,
                       'values': {}, 'provenance': {}}
                for feature, ttl in features.items():
                    safe = self._select(query, feature, ttl)
                    hindsight = self._select(query, feature, ttl, known=False)
                    row['values'][feature] = safe['value'] if safe else None
                    row['provenance'][feature] = safe
                    if hindsight and hindsight['available_at'] > query.at:
                        leaks.append({'query_id': query.id, 'feature': feature,
                                      'reason': 'event-time-only join uses unavailable data',
                                      'unsafe_event_id': hindsight['id'],
                                      'safe_event_id': safe['id'] if safe else None,
                                      'available_after_prediction_by': hindsight['available_at']-query.at})
                rows.append(row)
            digest = hashlib.sha256()
            for item in self.db.execute('SELECT * FROM events ORDER BY id'):
                digest.update(json.dumps(dict(item), sort_keys=True, separators=(',', ':'), allow_nan=False).encode()+b'\n')
            count = self.db.execute('SELECT COUNT(*) FROM events').fetchone()[0]
        return {'version': 1, 'feature_ttl': features,
                'manifest': {'event_count': count, 'events_sha256': digest.hexdigest(),
                             'query_count': len(queries),
                             'semantics': 'event_time <= at AND available_at <= at; inclusive TTL; newest event then newest known version'},
                'summary': {'feature_cells': len(queries)*len(features),
                            'missing_cells': sum(v is None for r in rows for v in r['values'].values()),
                            'leaky_cells_in_event_time_only_join': len(leaks)},
                'rows': rows, 'leakage': leaks}


def reference(events, queries, features):
    """Slow exhaustive oracle used only in tests/benchmarks, not in Store.build."""
    output = []
    for query in queries:
        row = {}
        for feature, ttl in features.items():
            eligible = [e for e in events if e.entity == query.entity and e.feature == feature
                        and e.event_time <= query.at and e.available_at <= query.at
                        and (ttl is None or query.at-e.event_time <= ttl)]
            best = max(eligible, key=lambda e: (e.event_time, e.available_at), default=None)
            row[feature] = best.id if best else None
        output.append(row)
    return output


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input', type=Path, help='JSON with events, queries and feature_ttl')
    parser.add_argument('--database', default=':memory:')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--fail-on-leakage', action='store_true', help='exit 2 when the audited event-time-only join leaks')
    args = parser.parse_args(argv)
    try:
        spec = json.loads(args.input.read_text())
        events = [Event(**row) for row in spec['events']]
        queries = [Query(**row) for row in spec['queries']]
        validate_features(spec['feature_ttl'])
        if len({q.id for q in queries}) != len(queries):
            raise ValueError('query IDs must be unique')
        with Store(args.database) as store:
            store.ingest(events)
            result = store.build(queries, spec['feature_ttl'])
        content = json.dumps(result, indent=2, allow_nan=False)+'\n'
        if args.output:
            args.output.write_text(content)
        else:
            print(content, end='')
        return 2 if args.fail_on_leakage and result['leakage'] else 0
    except (OSError, ValueError, TypeError, KeyError, sqlite3.Error) as error:
        parser.exit(1, f'featurefence: {error}\n')


if __name__ == '__main__':
    raise SystemExit(main())
