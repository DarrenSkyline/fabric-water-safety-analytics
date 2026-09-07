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

# # Bronze Data Ingestion
# 
# ## Purpose
# 
# This notebook loads synthetic and anonymised community water safety
# outreach data from CSV files stored in the Lakehouse raw files area.
# 
# The notebook:
# 
# 1. Defines explicit source schemas.
# 2. Reads four CSV source files.
# 3. Performs basic structural validation.
# 4. Adds ingestion metadata.
# 5. Writes the data to Bronze Delta tables.
# 
# ## Source files
# 
# - `Files/raw/events.csv`
# - `Files/raw/locations.csv`
# - `Files/raw/educators.csv`
# - `Files/raw/event_educators.csv`
# 
# ## Target tables
# 
# - `bronze_events`
# - `bronze_locations`
# - `bronze_educators`
# - `bronze_event_educators`
# 
# > Bronze tables preserve source values as strings. Business type
# > conversion and cleaning will be performed in the Silver layer.

# CELL ********************

# Import Spark component
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

from pyspark.sql.functions import (
    current_timestamp,
    lit
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define schema

events_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_group_id", StringType(), True),
    StructField("event_name", StringType(), True),
    StructField("event_date", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("programme_type", StringType(), True),
    StructField("location_id", StringType(), True),
    StructField("lead_educator_id", StringType(), True),
    StructField("delivery_language", StringType(), True),
    StructField("audience_type", StringType(), True),
    StructField("delivery_format", StringType(), True),
    StructField("demonstration_type", StringType(), True),
    StructField("registration_required", StringType(), True),
    StructField("registered_count", StringType(), True),
    StructField("attended_count", StringType(), True),
    StructField("engaged_people_count", StringType(), True),
    StructField("engaged_boats_count", StringType(), True),
    StructField("session_duration_minutes", StringType(), True),
    StructField("satisfaction_score", StringType(), True),
    StructField("engagement_level", StringType(), True),
    StructField("challenge_category", StringType(), True),
    StructField("improvement_category", StringType(), True),
    StructField("weather_condition", StringType(), True),
    StructField("data_source", StringType(), True),
    StructField("is_estimated", StringType(), True),
    StructField("event_status", StringType(), True)
])

locations_schema = StructType([
    StructField("location_id", StringType(), False),
    StructField("venue_name", StringType(), True),
    StructField("suburb", StringType(), True),
    StructField("region", StringType(), True),
    StructField("venue_type", StringType(), True),
    StructField("indoor_outdoor", StringType(), True),
    StructField("water_access", StringType(), True)
])

educators_schema = StructType([
    StructField("educator_id", StringType(), False),
    StructField("educator_name", StringType(), True),
    StructField("primary_language", StringType(), True),
    StructField("secondary_language", StringType(), True),
    StructField("experience_level", StringType(), True),
    StructField("active_status", StringType(), True)
])

event_educators_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("educator_id", StringType(), False),
    StructField("educator_role", StringType(), True),
    StructField("hours_worked", StringType(), True),
    StructField("is_estimated", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create function to read csv files

def read_raw_csv(file_path, schema, source_file):
    return (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("nullValue", "")
        .schema(schema)
        .csv(file_path)
        .withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
        .withColumn(
            "source_file",
            lit(source_file)
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read csv files

events_df = read_raw_csv(
    "Files/raw/events.csv",
    events_schema,
    "events.csv"
)

locations_df = read_raw_csv(
    "Files/raw/locations.csv",
    locations_schema,
    "locations.csv"
)

educators_df = read_raw_csv(
    "Files/raw/educators.csv",
    educators_schema,
    "educators.csv"
)

event_educators_df = read_raw_csv(
    "Files/raw/event_educators.csv",
    event_educators_schema,
    "event_educators.csv"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check read data

print(f"Events: {events_df.count()}")
print(f"Locations: {locations_df.count()}")
print(f"Educators: {educators_df.count()}")
print(
    f"Event-Educator assignments: "
    f"{event_educators_df.count()}"
)

# Auto check
expected_counts = {
    "events": 47,
    "locations": 27,
    "educators": 8,
    "event_educators": 132
}

actual_counts = {
    "events": events_df.count(),
    "locations": locations_df.count(),
    "educators": educators_df.count(),
    "event_educators": event_educators_df.count()
}

for dataset_name, expected_count in expected_counts.items():
    actual_count = actual_counts[dataset_name]

    assert actual_count == expected_count, (
        f"{dataset_name}: expected {expected_count} rows, "
        f"but found {actual_count}"
    )

    print(
        f"PASS - {dataset_name}: "
        f"{actual_count} rows"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# View schema and data

events_df.printSchema()

display(
    events_df.select(
        "event_id",
        "event_name",
        "event_date",
        "event_type",
        "location_id",
        "lead_educator_id",
        "engaged_people_count",
        "source_file",
        "ingestion_timestamp"
    ).limit(10)
)

display(
    event_educators_df
    .orderBy("event_id", "educator_id")
    .limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check for duplicate primary keys

duplicate_events_df = (
    events_df
    .groupBy("event_id")
    .count()
    .filter("count > 1")
)

duplicate_locations_df = (
    locations_df
    .groupBy("location_id")
    .count()
    .filter("count > 1")
)

duplicate_educators_df = (
    educators_df
    .groupBy("educator_id")
    .count()
    .filter("count > 1")
)

duplicate_assignments_df = (
    event_educators_df
    .groupBy("event_id", "educator_id")
    .count()
    .filter("count > 1")
)

assert duplicate_events_df.count() == 0, \
    "Duplicate event_id found"

assert duplicate_locations_df.count() == 0, \
    "Duplicate location_id found"

assert duplicate_educators_df.count() == 0, \
    "Duplicate educator_id found"

assert duplicate_assignments_df.count() == 0, \
    "Duplicate event-educator assignment found"

print("PASS - No duplicate primary keys found")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check for foreign keys

missing_event_locations_df = (
    events_df
    .select("location_id")
    .distinct()
    .join(
        locations_df.select("location_id"),
        on="location_id",
        how="left_anti"
    )
)

missing_lead_educators_df = (
    events_df
    .select("lead_educator_id")
    .distinct()
    .join(
        educators_df.select(
            educators_df.educator_id.alias(
                "lead_educator_id"
            )
        ),
        on="lead_educator_id",
        how="left_anti"
    )
)

missing_assignment_events_df = (
    event_educators_df
    .select("event_id")
    .distinct()
    .join(
        events_df.select("event_id"),
        on="event_id",
        how="left_anti"
    )
)

missing_assignment_educators_df = (
    event_educators_df
    .select("educator_id")
    .distinct()
    .join(
        educators_df.select("educator_id"),
        on="educator_id",
        how="left_anti"
    )
)

assert missing_event_locations_df.count() == 0, \
    "Some events reference missing locations"

assert missing_lead_educators_df.count() == 0, \
    "Some events reference missing lead educators"

assert missing_assignment_events_df.count() == 0, \
    "Some assignments reference missing events"

assert missing_assignment_educators_df.count() == 0, \
    "Some assignments reference missing educators"

print("PASS - All foreign key references are valid")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check whether there are at least one educators in each event

from pyspark.sql.functions import count, col

staffing_df = (
    event_educators_df
    .groupBy("event_id")
    .agg(count("educator_id").alias("educator_count"))
)

staffing_summary_df = (
    events_df
    .select(
        "event_id", 
        "event_name",
        "event_date",
        "event_type",
        "programme_type"
    )
    .join(
        staffing_df,
        on="event_id",
        how="left"
    )
)

display(staffing_summary_df)

# Check the boat show sraff number
boat_show_staffing_df = (
    staffing_summary_df
    .filter(col("event_name").contains("Hutchwilco Boat Show"))
    .select(
        "event_id",
        "event_date",
        "event_name",
        "educator_count"
    )
    .orderBy("event_date")
)

display(boat_show_staffing_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define function
def write_bronze_table(dataframe, table_name):
    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

    print(f"Create table: {table_name}")

# Create Bronze Delta Table
write_bronze_table(
    events_df,
    "bronze_events"
)

write_bronze_table(
    locations_df,
    "bronze_locations"
)

write_bronze_table(
    educators_df,
    "bronze_educators"
)

write_bronze_table(
    event_educators_df,
    "bronze_event_educators"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify write results

verification_df = spark.sql("""
    SELECT 'bronze_events' AS table_name,
        COUNT(*) AS row_count
    FROM bronze_events

    UNION ALL

    SELECT 'bronze_locations',
        COUNT(*)
    FROM bronze_locations

    UNION ALL

    SELECT 'bronze_educators',
        COUNT(*)
    FROM bronze_educators

    UNION ALL

    SELECT 'bronze_event_educators',
        COUNT(*)
    FROM bronze_event_educators
""")

display(verification_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Bronze Data Validation Summary

from pyspark.sql.functions import col, count

# ---------------------------------------------------------
# 1. Duplicate primary key checks
# ---------------------------------------------------------

duplicate_event_ids = (
    events_df
    .groupBy("event_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
    .count()
)

duplicate_location_ids = (
    locations_df
    .groupBy("location_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
    .count()
)

duplicate_educator_ids = (
    educators_df
    .groupBy("educator_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
    .count()
)

duplicate_event_educator_pairs = (
    event_educators_df
    .groupBy("event_id", "educator_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
    .count()
)


# ---------------------------------------------------------
# 2. Foreign key checks
# ---------------------------------------------------------

invalid_event_locations = (
    events_df
    .select("event_id", "location_id")
    .join(
        locations_df.select("location_id"),
        on="location_id",
        how="left_anti"
    )
    .count()
)

invalid_lead_educators = (
    events_df.alias("event")
    .join(
        educators_df.alias("educator"),
        col("event.lead_educator_id")
        == col("educator.educator_id"),
        how="left_anti"
    )
    .count()
)

invalid_bridge_events = (
    event_educators_df
    .select("event_id")
    .join(
        events_df.select("event_id"),
        on="event_id",
        how="left_anti"
    )
    .count()
)

invalid_bridge_educators = (
    event_educators_df
    .select("educator_id")
    .join(
        educators_df.select("educator_id"),
        on="educator_id",
        how="left_anti"
    )
    .count()
)

invalid_foreign_keys = (
    invalid_event_locations
    + invalid_lead_educators
    + invalid_bridge_events
    + invalid_bridge_educators
)


# ---------------------------------------------------------
# 3. Check that every event has at least one educator
# ---------------------------------------------------------

events_without_educators = (
    events_df
    .select("event_id")
    .join(
        event_educators_df
        .select("event_id")
        .distinct(),
        on="event_id",
        how="left_anti"
    )
    .count()
)


# ---------------------------------------------------------
# 4. Display validation summary
# ---------------------------------------------------------

print("Bronze Data Validation Summary")
print("----------------------------------------")
print(f"Duplicate event IDs: {duplicate_event_ids}")
print(f"Duplicate location IDs: {duplicate_location_ids}")
print(f"Duplicate educator IDs: {duplicate_educator_ids}")
print(
    "Duplicate event-educator pairs: "
    f"{duplicate_event_educator_pairs}"
)
print(f"Invalid foreign keys: {invalid_foreign_keys}")
print(
    "Events without educators: "
    f"{events_without_educators}"
)


# ---------------------------------------------------------
# 5. Stop the notebook if validation fails
# ---------------------------------------------------------

assert duplicate_event_ids == 0, \
    "Duplicate event_id values found."

assert duplicate_location_ids == 0, \
    "Duplicate location_id values found."

assert duplicate_educator_ids == 0, \
    "Duplicate educator_id values found."

assert duplicate_event_educator_pairs == 0, \
    "Duplicate event-educator pairs found."

assert invalid_foreign_keys == 0, \
    "Invalid foreign key references found."

assert events_without_educators == 0, \
    "Some events do not have an assigned educator."

print("----------------------------------------")
print("All Bronze validation checks passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
