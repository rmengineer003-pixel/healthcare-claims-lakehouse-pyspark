import argparse
import logging
import sys

import yaml
from pyspark.sql import SparkSession

from .pipeline import run
from .synthetic import generate


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Run the claims lakehouse pipeline locally")
    p.add_argument("--config", default="conf/local.yaml")
    p.add_argument("--generate", action="store_true", help="create synthetic landing data first")
    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    with open(a.config) as fh:
        cfg = yaml.safe_load(fh)
    if a.generate:
        generate(cfg["landing_path"], **cfg.get("synthetic", {}))
    spark = (SparkSession.builder.appName("claims-lakehouse").master(cfg.get("master", "local[*]"))
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        report = run(spark, cfg["landing_path"], cfg["output_path"], cfg.get("table_format", "parquet"))
    finally:
        spark.stop()
    print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
