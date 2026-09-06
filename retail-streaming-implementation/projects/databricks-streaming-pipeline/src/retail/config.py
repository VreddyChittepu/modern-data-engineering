import re
from dataclasses import dataclass

@dataclass(frozen=True)
class Config:
    catalog: str
    source_path: str
    schema_location: str
    watermark: str = "30 minutes"
    lateness_seconds: int = 1800
    trigger_interval: str = "30 seconds"
    tombstone_seconds: int = 604800

    @classmethod
    def from_spark(cls, spark):
        prefix = "retail."
        cfg = cls(
            catalog=spark.conf.get(prefix + "catalog"),
            source_path=spark.conf.get(prefix + "source_path"),
            schema_location=spark.conf.get(prefix + "schema_location"),
            watermark=spark.conf.get(prefix + "watermark", "30 minutes"),
            lateness_seconds=int(spark.conf.get(prefix + "lateness_seconds", "1800")),
            trigger_interval=spark.conf.get("pipelines.trigger.interval", "30 seconds"),
            tombstone_seconds=int(spark.conf.get(prefix + "tombstone_seconds", "604800")),
        )
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", cfg.catalog):
            raise ValueError("catalog must be a simple Unity Catalog identifier")
        return cfg

    def table(self, layer, name):
        return f"{self.catalog}.{layer}.{name}"
