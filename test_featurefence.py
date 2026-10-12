import json
from pathlib import Path
import random
import tempfile
import unittest

from featurefence import Event, Query, Store, main, reference


def event(id='a', time=10, available=12, value=5, entity='u', feature='x'):
    return Event(id, entity, feature, time, available, value)


class FeatureFenceTests(unittest.TestCase):
    def setUp(self):
        self.store = Store()
        self.addCleanup(self.store.close)

    def build(self, at=20, ttl=None):
        return self.store.build([Query('q', 'u', at)], {'x': ttl})

    def test_late_arrival_uses_older_known_value(self):
        self.store.ingest([event(), event('late', 18, 25, 99)])
        result = self.build()
        self.assertEqual(result['rows'][0]['values']['x'], 5)
        self.assertEqual(result['leakage'][0]['unsafe_event_id'], 'late')
        self.assertEqual(result['leakage'][0]['available_after_prediction_by'], 5)

    def test_unknown_value_remains_missing_not_zero(self):
        self.store.ingest([event(available=25)])
        result = self.build()
        self.assertIsNone(result['rows'][0]['values']['x'])
        self.assertEqual(result['summary']['missing_cells'], 1)

    def test_correction_only_after_available(self):
        self.store.ingest([event(), event('correction', 10, 21, 99)])
        self.assertEqual(self.build()['rows'][0]['values']['x'], 5)
        self.assertEqual(self.build(at=21)['rows'][0]['values']['x'], 99)

    def test_newer_event_beats_later_revision_of_old_event(self):
        self.store.ingest([event('newer', 15, 16, 1), event('revision', 10, 19, 2)])
        self.assertEqual(self.build()['rows'][0]['values']['x'], 1)

    def test_future_event_is_never_selected(self):
        self.store.ingest([event(time=21, available=21)])
        self.assertEqual(self.build()['summary']['missing_cells'], 1)
        self.assertEqual(self.build()['leakage'], [])

    def test_inclusive_event_and_availability_boundary(self):
        self.store.ingest([event(time=20, available=20)])
        self.assertEqual(self.build(ttl=0)['rows'][0]['values']['x'], 5)

    def test_ttl_inclusive_and_zero(self):
        self.store.ingest([event()])
        self.assertEqual(self.build(ttl=10)['summary']['missing_cells'], 0)
        self.assertEqual(self.build(ttl=9)['summary']['missing_cells'], 1)
        self.assertEqual(self.build(ttl=0)['summary']['missing_cells'], 1)

    def test_entities_and_features_are_isolated(self):
        self.store.ingest([event(entity='other'), event('y', feature='y')])
        self.assertIsNone(self.build()['rows'][0]['values']['x'])

    def test_exact_replay_is_idempotent(self):
        self.assertEqual(self.store.ingest([event()]), 1)
        self.assertEqual(self.store.ingest([event(value=5.)]), 0)

    def test_conflict_rolls_back_whole_batch(self):
        self.store.ingest([event()])
        with self.assertRaises(ValueError):
            self.store.ingest([event('new', 15, 15), event(value=99)])
        self.assertEqual(self.build()['manifest']['event_count'], 1)

    def test_ambiguous_versions_rejected(self):
        with self.assertRaises(ValueError):
            self.store.ingest([event(), event('other')])
        self.assertEqual(self.build()['manifest']['event_count'], 0)

    def test_ingestion_order_does_not_change_manifest_or_result(self):
        events = [event(), event('later', 15, 17)]
        self.store.ingest(events)
        with Store() as other:
            other.ingest(reversed(events))
            self.assertEqual(self.build(), other.build([Query('q', 'u', 20)], {'x': None}))

    def test_invalid_input(self):
        for values in ({'time': True}, {'time': -1}, {'available': 9}, {'value': float('nan')},
                       {'value': float('inf')}, {'value': 10**400}, {'value': True}, {'entity': ''}, {'time': 2**63}):
            with self.subTest(values=values), self.assertRaises(ValueError):
                event(**values)
        for ttl in (-1, True, '10'):
            with self.assertRaises(ValueError):
                self.build(ttl=ttl)

    def test_duplicate_queries_fail_and_order_is_preserved(self):
        with self.assertRaises(ValueError):
            self.store.build([Query('q', 'u', 1), Query('q', 'u', 2)], {'x': None})
        result = self.store.build([Query('z', 'u', 2), Query('a', 'u', 1)], {'x': None})
        self.assertEqual([r['query_id'] for r in result['rows']], ['z', 'a'])

    def test_query_values_are_parameterized(self):
        self.store.ingest([event(entity="'; DROP TABLE events; --")])
        self.assertEqual(self.build()['manifest']['event_count'], 1)

    def test_persistence_after_reopen(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'store.sqlite'
            with Store(path) as store:
                store.ingest([event()])
            with Store(path) as store:
                self.assertEqual(store.build([Query('q', 'u', 20)], {'x': None})['rows'][0]['values']['x'], 5)

    def test_build_keeps_one_snapshot_during_concurrent_append(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'store.sqlite'
            with Store(path) as reader, Store(path) as writer:
                reader.ingest([event()])
                original = reader._select
                done = False
                def select(*args, **kwargs):
                    nonlocal done
                    result = original(*args, **kwargs)
                    if not done:
                        done = True
                        writer.ingest([event('new', 19, 19, 99)])
                    return result
                reader._select = select
                result = reader.build([Query('a', 'u', 20), Query('b', 'u', 20)], {'x': None})
                self.assertEqual([r['values']['x'] for r in result['rows']], [5, 5])
                self.assertEqual(result['manifest']['event_count'], 1)
                self.assertEqual(reader.build([], {'x': None})['manifest']['event_count'], 2)

    def test_randomized_against_exhaustive_oracle(self):
        for seed in range(30):
            rng = random.Random(seed)
            events = [event(str(i), i*2, i*2+rng.randrange(20), rng.random(),
                            entity=str(rng.randrange(4)), feature=str(rng.randrange(3))) for i in range(150)]
            queries = [Query(str(i), str(rng.randrange(4)), rng.randrange(330)) for i in range(25)]
            features = {'0': None, '1': 25, '2': 0}
            with Store() as store:
                store.ingest(events)
                result = store.build(queries, features)
                ids = [{k: v['id'] if v else None for k, v in row['provenance'].items()} for row in result['rows']]
                self.assertEqual(ids, reference(events, queries, features))
                expected_leaks = []
                for query in queries:
                    for feature, ttl in features.items():
                        candidates = [e for e in events if e.entity == query.entity and e.feature == feature
                                      and e.event_time <= query.at and (ttl is None or query.at-e.event_time <= ttl)]
                        winner = max(candidates, key=lambda e: (e.event_time, e.available_at), default=None)
                        if winner and winner.available_at > query.at:
                            expected_leaks.append((query.id, feature, winner.id))
                self.assertEqual([(r['query_id'], r['feature'], r['unsafe_event_id']) for r in result['leakage']], expected_leaks)

    def test_lookup_uses_composite_index(self):
        plan = self.store.db.execute('EXPLAIN QUERY PLAN SELECT * FROM events WHERE entity=? AND feature=? AND event_time BETWEEN ? AND ? AND available_at<=? ORDER BY event_time DESC, available_at DESC LIMIT 1', ('u', 'x', 0, 20, 20)).fetchall()
        self.assertTrue(any('point_in_time' in row[3] for row in plan))

    def test_demo_and_ci_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)/'result.json'
            args = ['example.json', '--output', str(output)]
            self.assertEqual(main(args), 0)
            report = json.loads(output.read_text())
            self.assertEqual(report['summary']['leaky_cells_in_event_time_only_join'], 2)
            self.assertEqual(main(args+['--fail-on-leakage']), 2)


if __name__ == '__main__':
    unittest.main()
