import json
from datetime import datetime, timedelta
import pytest
from retail.transforms import deduplicate

@pytest.mark.streaming
def test_watermark_dedup_and_checkpoint_restart(spark, tmp_path):
    source = tmp_path / "input"; source.mkdir()
    output = tmp_path / "output"
    checkpoint = tmp_path / "checkpoint"
    base = datetime(2026, 9, 6, 10)
    def write(n, records):
        (source / f"{n}.json").write_text("\n".join(json.dumps({"event_id": k,
            "event_timestamp": (base+timedelta(minutes=m)).isoformat()}) for k,m in records))
    def run():
        data = spark.readStream.schema("event_id string, event_timestamp timestamp").json(str(source))
        q = (deduplicate(data).writeStream.format("parquet").option("path", str(output))
            .option("checkpointLocation", str(checkpoint)).trigger(availableNow=True).start())
        q.awaitTermination(60)
        assert not q.isActive
    write(1, [("one",0), ("one",0), ("two",5)])
    run()
    write(2, [("one",0), ("future",90)])
    run()
    write(3, [("too-late",1), ("new",95)])
    run()
    ids = {r.event_id for r in spark.read.parquet(str(output)).collect()}
    assert ids == {"one", "two", "future", "new"}
    assert spark.read.parquet(str(output)).count() == 4
    run()  # unchanged source + checkpoint does not write another copy
    assert spark.read.parquet(str(output)).count() == 4
