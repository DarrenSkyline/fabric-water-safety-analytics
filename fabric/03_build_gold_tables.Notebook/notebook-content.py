# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e02a6e1d-c2f6-4f22-b451-03680f20c27d",
# META       "default_lakehouse_name": "water_safety_lakehouse",
# META       "default_lakehouse_workspace_id": "decaba21-a539-4fd4-85e4-5d1ffa1c14d2",
# META       "known_lakehouses": [
# META         {
# META           "id": "e02a6e1d-c2f6-4f22-b451-03680f20c27d"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Gold Analytical Model
# 
# ## Purpose
# 
# This notebook transforms analysis-ready Silver data into a
# business-focused Gold dimensional model.
# 
# The Gold layer is designed to support:
# 
# - Business reporting
# - Direct Lake semantic modeling
# - DAX measures
# - Power BI dashboards
# - Regional and programme analysis
# - Educator activity analysis
# 
# ## Gold Tables
# 
# - `gold_fact_event_delivery`
# - `gold_dim_date`
# - `gold_dim_location`
# - `gold_dim_educator`
# - `gold_bridge_event_educator`
# 
# ## Grain
# 
# `gold_fact_event_delivery` contains one row per event delivery record.
# 
# `event_group_id` identifies related delivery records belonging to the
# same overall event or programme activity.

# CELL ********************

# Import

from pyspark.sql.functions import (
    col,
    lit,
    when,
    coalesce,
    current_timestamp,
    date_format,
    year,
    month,
    quarter,
    weekofyear,
    dayofmonth,
    explode,
    sequence,
    min as spark_min,
    max as spark_max,
    countDistinct,
    concat,
    round as spark_round
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read Silver tables

silver_events_df = spark.table("silver_events")
silver_locations_df = spark.table("silver_locations")
silver_educators_df = spark.table("silver_educators")
silver_event_educators_df = spark.table(
    "silver_event_educators"
)

print("Silver tables loaded successfully.")
print("----------------------------------------")
print(f"Events: {silver_events_df.count()}")
print(f"Locations: {silver_locations_df.count()}")
print(f"Educators: {silver_educators_df.count()}")
print(
    f"Event educators: "
    f"{silver_event_educators_df.count()}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create core Fact Table

gold_fact_event_delivery_df = (
    silver_events_df

    # Date key for dimensional model
    .withColumn(
        "event_date_key",
        date_format(
            col("event_date"),
            "yyyyMMdd"
        ).cast("int")
    )

    # Unified people-reached metric
    .withColumn(
        "people_reached",
        coalesce(
            col("engaged_people_count"),
            col("attended_count"),
            lit(0)
        )
    )

    # One record represents one delivery
    .withColumn(
        "delivery_count",
        lit(1)
    )

    # Attendance difference
    .withColumn(
        "attendance_variance",
        when(
            col("registered_count").isNotNull()
            & col("attended_count").isNotNull(),
            col("attended_count")
            - col("registered_count")
        )
    )

    # Attendance rate can legitimately exceed 100%
    .withColumn(
        "attendance_rate",
        when(
            col("registered_count").isNotNull()
            & (col("registered_count") > 0)
            & col("attended_count").isNotNull(),

            spark_round(
                col("attended_count")
                / col("registered_count"),
                4
            )
        )
    )

    # Convert duration to hours
    .withColumn(
        "session_duration_hours",
        spark_round(
            col("session_duration_minutes") / 60.0,
            2
        )
    )

    .withColumn(
        "gold_processed_timestamp",
        current_timestamp()
    )

    .select(
        "event_id",
        "event_group_id",
        "event_date_key",
        "event_date",
        "location_id",
        "lead_educator_id",

        "event_name",
        "event_type",
        "programme_type",

        "delivery_language",
        "audience_type",
        "delivery_format",
        "demonstration_type",

        "registration_required",
        "registered_count",
        "attended_count",
        "attendance_variance",
        "attendance_rate",

        "engaged_people_count",
        "people_reached",
        "engaged_boats_count",

        "session_duration_minutes",
        "session_duration_hours",

        "satisfaction_score",
        "engagement_level",

        "weather_condition",
        "is_estimated",
        "event_status",

        "delivery_count",
        "gold_processed_timestamp"
    )
)

display(gold_fact_event_delivery_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Location Dimension

gold_dim_location_df = (
    silver_locations_df

    # create business-friendly venue category
    .withColumn(
        "venue_category",
        when(
            col("venue_type").isin(
                "Church",
                "Church Hall"
            ),
            lit("Church")
        ).otherwise(
            col("venue_type")
        )
    )

    .select(
        "location_id",
        "venue_name",
        "suburb",
        "region",
        "venue_type",
        "venue_category",
        "indoor_outdoor",
        "water_access"
    )
    .dropDuplicates(["location_id"])
    .withColumn(
        "gold_processed_timestamp",
        current_timestamp()
    )
)

display(gold_dim_location_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Educator Dimension

gold_dim_educator_df = (
    silver_educators_df
    .select(
        "educator_id",
        "educator_name",
        "primary_language",
        "secondary_language",
        "experience_level",
        "active_status"
    )
    .dropDuplicates(["educator_id"])
    .withColumn(
        "gold_processed_timestamp",
        current_timestamp()
    )
)

display(gold_dim_educator_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Date Dimension

date_bounds = (
    silver_events_df
    .agg(
        spark_min("event_date").alias("min_date"),
        spark_max("event_date").alias("max_date")
    )
    .first()
)

date_range_df = spark.createDataFrame(
    [
        (
            date_bounds["min_date"],
            date_bounds["max_date"]
        )
    ],
    [
        "start_date",
        "end_date"
    ]
)

gold_dim_date_df = (
    date_range_df

    .select(
        explode(
            sequence(
                col("start_date"),
                col("end_date")
            )
        ).alias("date")
    )

    .withColumn(
        "date_key",
        date_format(
            col("date"),
            "yyyyMMdd"
        ).cast("int")
    )

    .withColumn(
        "year",
        year("date")
    )

    .withColumn(
        "quarter",
        concat(
            lit("Q"),
            quarter("date")
        )
    )

    .withColumn(
        "month_number",
        month("date")
    )

    .withColumn(
        "month_name",
        date_format(
            col("date"),
            "MMMM"
        )
    )

    .withColumn(
        "year_month",
        date_format(
            col("date"),
            "yyyy-MM"
        )
    )

    .withColumn(
        "week_of_year",
        weekofyear("date")
    )

    .withColumn(
        "day_of_month",
        dayofmonth("date")
    )

    .withColumn(
        "day_name",
        date_format(
            col("date"),
            "EEEE"
        )
    )

    .select(
        "date_key",
        "date",
        "year",
        "quarter",
        "month_number",
        "month_name",
        "year_month",
        "week_of_year",
        "day_of_month",
        "day_name"
    )
)

display(gold_dim_date_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Event - Educator Bridge

gold_bridge_event_educator_df = (
    silver_event_educators_df

    .select(
        "event_id",
        "educator_id",
        "educator_role",
        "hours_worked",
        "is_estimated" 
    )

    .dropDuplicates([
        "event_id",
        "educator_id"
    ])

    .withColumn(
        "gold_processed_timestamp",
        current_timestamp()
    )
)

display(
    gold_bridge_event_educator_df.limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gold Validation

fact_count = gold_fact_event_delivery_df.count()
location_count = gold_dim_location_df.count()
educator_count = gold_dim_educator_df.count()
bridge_count = gold_bridge_event_educator_df.count()
date_count = gold_dim_date_df.count()

event_group_count = (
    gold_fact_event_delivery_df
    .select("event_group_id")
    .distinct()
    .count()
)

events_without_locations = (
    gold_fact_event_delivery_df
    .select("location_id")
    .join(
        gold_dim_location_df.select("location_id"),
        on="location_id",
        how="left_anti"
    )
    .count()
)

bridge_without_events = (
    gold_bridge_event_educator_df
    .select("event_id")
    .join(
        gold_fact_event_delivery_df.select("event_id"),
        on="event_id",
        how="left_anti"
    )
    .count()
)

bridge_without_educators = (
    gold_bridge_event_educator_df
    .select("educator_id")
    .join(
        gold_dim_educator_df.select("educator_id"),
        on="educator_id",
        how="left_anti"
    )
    .count()
)

events_without_staff = (
    gold_fact_event_delivery_df
    .select("event_id")
    .join(
        gold_bridge_event_educator_df
        .select("event_id")
        .distinct(),
        on="event_id",
        how="left_anti"
    )
    .count()
)

print("Gold Data Model Validation")
print("----------------------------------------")
print(f"Delivery records: {fact_count}")
print(f"Event groups: {event_group_count}")
print(f"Locations: {location_count}")
print(f"Educators: {educator_count}")
print(f"Event-educator records: {bridge_count}")
print(f"Date records: {date_count}")
print("----------------------------------------")
print(
    f"Events without valid location: "
    f"{events_without_locations}"
)
print(
    f"Bridge records without event: "
    f"{bridge_without_events}"
)
print(
    f"Bridge records without educator: "
    f"{bridge_without_educators}"
)
print(
    f"Events without educator: "
    f"{events_without_staff}"
)

assert fact_count == 47
assert event_group_count == 34
assert location_count == 27
assert educator_count == 8
assert bridge_count == 132

assert events_without_locations == 0
assert bridge_without_events == 0
assert bridge_without_educators == 0
assert events_without_staff == 0

print("----------------------------------------")
print("All Gold validation checks passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write into Gold Delta Tables

def write_gold_table(dataframe, table_name):
    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

    print(f"Created Gold table: {table_name}")

write_gold_table(
    gold_fact_event_delivery_df,
    "gold_fact_event_delivery"
)

write_gold_table(
    gold_dim_date_df,
    "gold_dim_date"
)

write_gold_table(
    gold_dim_location_df,
    "gold_dim_location"
)

write_gold_table(
    gold_dim_educator_df,
    "gold_dim_educator"
)

write_gold_table(
    gold_bridge_event_educator_df,
    "gold_bridge_event_educator"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
