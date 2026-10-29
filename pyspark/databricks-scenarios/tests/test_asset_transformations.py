"""Runtime integration tests: run through notebook 44's asset-backed fixture plugin.

These tests intentionally cannot be treated as local Spark tests. Install pytest
in the Databricks notebook environment and use the runner in notebook 44.
"""


def test_output_schema(taxi_asset, trip_transform):
    out=trip_transform(taxi_asset)
    assert out.columns==['trip_id','pickup_zip','fare','distance','ratio']
    actual={f.name:f.dataType.simpleString() for f in out.schema.fields}
    assert actual=={'trip_id':'string','pickup_zip':'string','fare':'double','distance':'double','ratio':'double'}


def test_value_contract(taxi_asset, trip_transform):
    out=trip_transform(taxi_asset)
    assert out.filter('distance <= 0 OR fare < 0 OR ratio IS NULL').count()==0
    assert out.count()<=taxi_asset.count()


def test_deterministic_multiset(taxi_asset, trip_transform):
    first=trip_transform(taxi_asset)
    second=trip_transform(taxi_asset)
    assert first.exceptAll(second).limit(1).count()==0
    assert second.exceptAll(first).limit(1).count()==0


def test_replay_multiplicity(taxi_asset, trip_transform):
    once=trip_transform(taxi_asset)
    replay=trip_transform(taxi_asset.unionByName(taxi_asset))
    assert replay.count()==2*once.count()
    assert replay.dropDuplicates(['trip_id']).count()==once.dropDuplicates(['trip_id']).count()


def test_all_filtered_rows_accounted_for(taxi_asset, trip_transform):
    expected=taxi_asset.filter('distance > 0 AND fare >= 0').count()
    assert trip_transform(taxi_asset).count()==expected
