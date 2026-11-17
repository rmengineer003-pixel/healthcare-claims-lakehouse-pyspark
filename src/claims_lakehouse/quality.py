"""Lightweight, declarative data quality checks. Failing rows are quarantined, not dropped silently."""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class Rule:
    name: str
    condition: Column  # True means the row passes


def apply_rules(df: DataFrame, rules: list[Rule]) -> tuple[DataFrame, DataFrame]:
    """Return (valid, quarantined). Quarantined rows carry a `dq_failures` array of rule names."""
    failures = F.array_compact(F.array(*[F.when(~r.condition, F.lit(r.name)) for r in rules]))
    tagged = df.withColumn("dq_failures", failures)
    valid = tagged.filter(F.size("dq_failures") == 0).drop("dq_failures")
    quarantined = tagged.filter(F.size("dq_failures") > 0)
    return valid, quarantined


def summarize(quarantined: DataFrame) -> dict[str, int]:
    rows = quarantined.select(F.explode("dq_failures").alias("rule")).groupBy("rule").count().collect()
    return {r["rule"]: r["count"] for r in rows}
