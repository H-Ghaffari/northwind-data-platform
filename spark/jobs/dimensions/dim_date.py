"""Generate DimDate.

The only dimension with no source system: a calendar is derived, not
extracted. One row per day across the range the warehouse needs to report
on.

date_key is a smart key in yyyyMMdd form (19960704). Facts build it
directly from a date with toYYYYMMDD(), so no lookup against this table is
ever needed — the one place in the model where a surrogate key is derived
rather than assigned.
"""

from __future__ import annotations

import sys
from datetime import date

from pyspark.sql import functions as F

from common.spark_utils import log, spark_session, truncate_dw_table, write_to_dw

# Northwind's orders run from mid-1996 to mid-1998. The range is widened at
# both ends so late-arriving or back-dated rows still find a calendar row,
# and so the warehouse keeps working without regeneration for years.
START_DATE = date(1990, 1, 1)
END_DATE = date(2035, 12, 31)

TARGET_TABLE = "DimDate"

COLUMNS = [
    "date_key",
    "full_date",
    "calendar_year",
    "calendar_season",
    "season_name",
    "month_number_of_year",
    "month_name",
    "day_number_of_month",
    "day_of_week",
    "day_of_week_name",
]


def build_calendar(spark):
    """One row per day between START_DATE and END_DATE."""
    days = (END_DATE - START_DATE).days + 1
    log.info("Generating %s days: %s to %s", f"{days:,}", START_DATE, END_DATE)

    df = (
        spark.range(0, days)
        .withColumn("full_date", F.expr(f"date_add(to_date('{START_DATE}'), CAST(id AS INT))"))
        .drop("id")
    )

    # Meteorological seasons: December belongs to the following winter, which
    # is why the month is shifted by one before dividing into quarters.
    season_number = F.floor(((F.month("full_date") % 12) / 3)) + 1

    return (
        df.withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
          .withColumn("calendar_year", F.year("full_date").cast("smallint"))
          .withColumn("calendar_season", season_number.cast("tinyint"))
          .withColumn(
              "season_name",
              F.when(season_number == 1, "Winter")
               .when(season_number == 2, "Spring")
               .when(season_number == 3, "Summer")
               .otherwise("Autumn"),
          )
          .withColumn("month_number_of_year", F.month("full_date").cast("tinyint"))
          .withColumn("month_name", F.date_format("full_date", "MMMM"))
          .withColumn("day_number_of_month", F.dayofmonth("full_date").cast("tinyint"))
          .withColumn("day_of_week", F.dayofweek("full_date").cast("smallint"))
          .withColumn("day_of_week_name", F.date_format("full_date", "EEEE"))
          .select(*COLUMNS)
          .orderBy("date_key")
    )


def main() -> int:
    with spark_session("generate_dim_date") as spark:
        calendar = build_calendar(spark)

        log.info("Sample of what will be loaded:")
        calendar.filter(F.col("date_key").between(19960701, 19960707)).show(truncate=False)

        # Full reload rather than an incremental merge: a calendar is
        # deterministic, so regenerating it can never lose information.
        truncate_dw_table(TARGET_TABLE)
        written = write_to_dw(calendar, TARGET_TABLE, COLUMNS)

        log.info("DimDate loaded with %s rows", f"{written:,}")
        return written


if __name__ == "__main__":
    rows = main()
    sys.exit(0 if rows else 1)
