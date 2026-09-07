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

# # Silver Data Cleaning and Transformation
# 
# ## Purpose
# 
# This notebook transforms raw Bronze Lakehouse tables into clean,
# analysis-ready Silver Delta tables.
# 
# The notebook:
# 
# 1. Reads Bronze Delta tables.
# 2. Cleans and standardises text values.
# 3. Converts columns to appropriate data types.
# 4. Standardises selected categorical values.
# 5. Removes duplicate records.
# 6. Performs data quality and relationship validation.
# 7. Writes cleaned data to Silver Delta tables.
# 
# ## Source Tables
# 
# - `bronze_events`
# - `bronze_locations`
# - `bronze_educators`
# - `bronze_event_educators`
# 
# ## Target Tables
# 
# - `silver_events`
# - `silver_locations`
# - `silver_educators`
# - `silver_event_educators`

# CELL ********************

# Imports

from pyspark.sql.functions import (
    col,
    trim,
    when,
    lit,
    current_timestamp,
    count
)

from pyspark.sql.types import(
    IntegerType,
    DoubleType,
    BooleanType
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read Bronze Tables

bronze_events_df = spark.table("bronze_events")
bronze_locations_df = spark.table("bronze_locations")
bronze_educators_df = spark.table("bronze_educators")
bronze_event_educators_df = spark.table("bronze_event_educators")

print("Bronze tables loaded successfully.")
print("----------------------------------------")
print(f"Events: {bronze_events_df.count()}")
print(f"Locations: {bronze_locations_df.count()}")
print(f"Educators: {bronze_educators_df.count()}")
print(
    f"Event educators: "
    f"{bronze_event_educators_df.count()}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean Events

events_string_columns = [
    "event_id",
    "event_group_id",
    "event_name",
    "event_type",
    "programme_type",
    "location_id",
    "lead_educator_id",
    "delivery_language",
    "audience_type",
    "delivery_format",
    "demonstration_type",
    "engagement_level",
    "challenge_category",
    "improvement_category",
    "weather_condition",
    "data_source",
    "event_status"
]

silver_events_df = bronze_events_df

# Trim whitespace and convert empty strings to null
for colum_name in events_string_columns:
    silver_events_df = silver_events_df.withColumn(
        colum_name,
        when(
            trim(col(colum_name)) == "",
            None
        ).otherwise(trim(col(colum_name)))
    )

# Convert date
silver_events_df = silver_events_df.withColumn(
    "event_date",
    col("event_date").cast("date")
)

# Convert integer columns
integer_columns = [
    "registered_count",
    "attended_count",
    "engaged_people_count",
    "engaged_boats_count",
    "session_duration_minutes"
]

for colum_name in integer_columns:
    silver_events_df = silver_events_df.withColumn(
        colum_name,
        col(colum_name).cast(IntegerType())
    )

# Convert decimal
silver_events_df = silver_events_df.withColumn(
    "satisfaction_score",
    col("satisfaction_score").cast(DoubleType())
)

# Convert booleans
silver_events_df = (
    silver_events_df
    .withColumn(
        "registration_required",
        col("registration_required").cast(BooleanType())
    )
    .withColumn(
        "is_estimated",
        col("is_estimated").cast(BooleanType())
    )
)

# Standardise terminology
silver_events_df = silver_events_df.withColumn(
    "delivery_language",
    when(
        col("delivery_language") == "English/Te Reo Maori",
        "English/Te Reo Māori"
    ).otherwise(col("delivery_language"))
)

# Defensive duplicate removal
silver_events_df = (
    silver_events_df
    .dropDuplicates(["event_id"])
    .withColumn(
        "silver_processed_timestamp",
        current_timestamp()
    )
)
    
print("silver_events cleaned.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean Locations

location_string_columns = [
    "location_id",
    "venue_name",
    "suburb",
    "region",
    "venue_type",
    "indoor_outdoor",
    "water_access"
]

silver_locations_df = bronze_locations_df

for colum_name in location_string_columns:
    silver_locations_df = silver_locations_df.withColumn(
        colum_name,
        when(
            trim(col(colum_name)) == "",
            None
        ).otherwise(trim(col(colum_name)))
    )

silver_locations_df = (
    silver_locations_df
    .dropDuplicates(["location_id"])
    .withColumn(
        "silver_processed_timestamp",
        current_timestamp()
    )
)

print("silver_locations cleaned.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean Educators

educator_string_columns = [
    "educator_id",
    "educator_name",
    "primary_language",
    "secondary_language",
    "experience_level"
]

silver_educators_df = bronze_educators_df

for colum_name in educator_string_columns:
    silver_educators_df = silver_educators_df.withColumn(
        colum_name,
        when(
            trim(col(colum_name)) == "",
            None
        ).otherwise(trim(col(colum_name)))
    )

# Standardise terminology
silver_educators_df = silver_educators_df.withColumn(
    "secondary_language",
    when(
        col("secondary_language") == "Te Reo Maori",
        "Te Reo Māori"
    ).otherwise(col("secondary_language"))
)

# Convert boolean
silver_educators_df = silver_educators_df.withColumn(
    "active_status",
    col("active_status").cast(BooleanType())
)

silver_educators_df = (
    silver_educators_df
    .dropDuplicates(["educator_id"])
    .withColumn(
        "silver_processed_timestamp",
        current_timestamp()
    )
)

print("silver_educators cleaned.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean Event Educators

silver_event_educators_df = (
    bronze_event_educators_df

    .withColumn(
        "event_id",
        trim(col("event_id"))
    )

    .withColumn(
        "educator_id",
        trim(col("educator_id"))
    )

    .withColumn(
        "educator_role",
        trim(col("educator_role"))
    )

    .withColumn(
        "hours_worked",
        col("hours_worked").cast(DoubleType())
    )

    .withColumn(
        "is_estimated",
        col("is_estimated").cast(BooleanType())
    )

    .dropDuplicates([
        "event_id",
        "educator_id"
    ])

    .withColumn(
        "silver_processed_timestamp",
        current_timestamp()
    )
)

print("silver_event_educators cleaned.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check Schema

print("SILVER EVENTS SCHEMA")
silver_events_df.printSchema()

print("\nSILVER EVENTS EDUCATORS SCHEMA")
silver_event_educators_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Silver Data Quality Validation

# Row counts
event_count = silver_events_df.count()
location_count = silver_locations_df.count()
educator_count = silver_educators_df.count()
event_educator_count = silver_event_educators_df.count()


# Null primary keys
null_event_ids = (
    silver_events_df
    .filter(col("event_id").isNull())
    .count()
)

null_location_ids = (
    silver_locations_df
    .filter(col("location_id").isNull())
    .count()
)

null_educator_ids = (
    silver_educators_df
    .filter(col("educator_id").isNull())
    .count()
)


# Invalid location foreign keys
invalid_locations = (
    silver_events_df
    .select("location_id")
    .join(
        silver_locations_df.select("location_id"),
        on="location_id",
        how="left_anti"
    )
    .count()
)


# Invalid educator foreign keys
invalid_educators = (
    silver_event_educators_df
    .select("educator_id")
    .join(
        silver_educators_df.select("educator_id"),
        on="educator_id",
        how="left_anti"
    )
    .count()
)


# Events without educator assignment
events_without_educators = (
    silver_events_df
    .select("event_id")
    .join(
        silver_event_educators_df
        .select("event_id")
        .distinct(),
        on="event_id",
        how="left_anti"
    )
    .count()
)


# Invalid satisfaction scores
invalid_satisfaction_scores = (
    silver_events_df
    .filter(
        col("satisfaction_score").isNotNull()
        & (
            (col("satisfaction_score") < 1)
            | (col("satisfaction_score") > 5)
        )
    )
    .count()
)


print("Silver Data Validation Summary")
print("----------------------------------------")
print(f"Events: {event_count}")
print(f"Locations: {location_count}")
print(f"Educators: {educator_count}")
print(f"Event educators: {event_educator_count}")
print("----------------------------------------")
print(f"Null event IDs: {null_event_ids}")
print(f"Null location IDs: {null_location_ids}")
print(f"Null educator IDs: {null_educator_ids}")
print(f"Invalid location references: {invalid_locations}")
print(f"Invalid educator references: {invalid_educators}")
print(f"Events without educators: {events_without_educators}")
print(
    f"Invalid satisfaction scores: "
    f"{invalid_satisfaction_scores}"
)


assert event_count == 47
assert location_count == 27
assert educator_count == 8
assert event_educator_count == 132

assert null_event_ids == 0
assert null_location_ids == 0
assert null_educator_ids == 0

assert invalid_locations == 0
assert invalid_educators == 0
assert events_without_educators == 0

assert invalid_satisfaction_scores == 0

print("----------------------------------------")
print("All Silver validation checks passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write into Silver Delta Tables

def write_silver_table(dataframe, table_name):
    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

    print(f"Created Silver table: {table_name}")


write_silver_table(
    silver_events_df,
    "silver_events"
)

write_silver_table(
    silver_locations_df,
    "silver_locations"
)

write_silver_table(
    silver_educators_df,
    "silver_educators"
)

write_silver_table(
    silver_event_educators_df,
    "silver_event_educators"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
