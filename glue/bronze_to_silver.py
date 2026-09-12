# ------------------------------------------------------------
# 1. IMPORT REQUIRED LIBRARIES
# ------------------------------------------------------------
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# ------------------------------------------------------------
# 2. INITIALISE AWS GLUE AND APACHE SPARK
# ------------------------------------------------------------
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ------------------------------------------------------------
# 3. DEFINE THE BRONZE GLUE DATABASE
# ------------------------------------------------------------
bronze_db = "eurostar_bronze_db"

# ------------------------------------------------------------
# 4. READ STATIONS TABLE
# ------------------------------------------------------------
stations = glueContext.create_dynamic_frame.from_catalog(
    database=bronze_db,
    table_name="stations"
).toDF()

# ------------------------------------------------------------
# 5. READ TRAINS TABLE
# ------------------------------------------------------------
trains = glueContext.create_dynamic_frame.from_catalog(
    database = bronze_db,
    table_name="trains"
).toDF()


# ------------------------------------------------------------
# 6. READ JOURNEYS TABLE
# ------------------------------------------------------------

journeys = glueContext.create_dynamic_frame.from_catalog(
    database = bronze_db,
    table_name = "journeys"
).toDF()

# ------------------------------------------------------------
# 7. READ BOOKINGS TABLE
# ------------------------------------------------------------

bookings = glueContext.create_dynamic_frame.from_catalog(
    database = bronze_db,
    table_name = "bookings"
).toDF()

# ------------------------------------------------------------
# 8. READ PAYMENTS TABLE
# ------------------------------------------------------------

payments = glueContext.create_dynamic_frame.from_catalog(
    database = bronze_db,
    table_name = "payments"
).toDF()

# ------------------------------------------------------------
# 9. READ DISRUPTIONS TABLE
# ------------------------------------------------------------

disruptions = glueContext.create_dynamic_frame.from_catalog(
    database = bronze_db,
    table_name = "disruptions"
).toDF()

# ------------------------------------------------------------
# 10. READ REFUNDS TABLE
# ------------------------------------------------------------

refunds = glueContext.create_dynamic_frame.from_catalog(
    database = bronze_db,
    table_name = "refunds"
).toDF()

# ------------------------------------------------------------
# 11. VERIFY THAT ALL BRONZE TABLES WERE LOADED
# ------------------------------------------------------------
print("Bronze tables loaded successfully")

# ------------------------------------------------------------
# 12. CHECK THE NUMBER OF RECORDS IN EACH TABLE
# ------------------------------------------------------------
print("Stations:", stations.count())
print("Trains:", trains.count())
print("Journeys:", journeys.count())
print("Bookings:", bookings.count())
print("Payments:", payments.count())
print("Disruptions:", disruptions.count())
print("Refunds:", refunds.count())




# ============================================================
# PART 2 - RENAME BRONZE COLUMNS TO BUSINESS-FRIENDLY NAMES
# ============================================================

# ------------------------------------------------------------
# 13. RENAME STATIONS COLUMNS
# ------------------------------------------------------------

stations = (
    stations
    .withColumnRenamed("col0", "station_id")
    .withColumnRenamed("col1", "station_name")
    .withColumnRenamed("col2", "city")
    .withColumnRenamed("col3", "country")
)


# ------------------------------------------------------------
# 14. RENAME TRAINS COLUMNS
# ------------------------------------------------------------

trains = (
    trains
    .withColumnRenamed("col0", "train_id")
    .withColumnRenamed("col1", "train_number")
    .withColumnRenamed("col2", "train_model")
    .withColumnRenamed("col3", "capacity")
    .withColumnRenamed("col4", "service_start_year")
)


# ------------------------------------------------------------
# 15. RENAME JOURNEYS COLUMNS
# ------------------------------------------------------------

journeys = (
    journeys
    .withColumnRenamed("col0", "journey_id")
    .withColumnRenamed("col1", "train_id")
    .withColumnRenamed("col2", "origin_station_id")
    .withColumnRenamed("col3", "destination_station_id")
    .withColumnRenamed("col4", "journey_date")
    .withColumnRenamed("col5", "scheduled_departure")
    .withColumnRenamed("col6", "scheduled_arrival")
    .withColumnRenamed("col7", "actual_arrival")
    .withColumnRenamed("col8", "journey_status")
    .withColumnRenamed("col9", "delay_minutes")
)


# ------------------------------------------------------------
# 16. RENAME BOOKINGS COLUMNS
# ------------------------------------------------------------

bookings = (
    bookings
    .withColumnRenamed("col0", "booking_id")
    .withColumnRenamed("col1", "journey_id")
    .withColumnRenamed("col2", "booking_date")
    .withColumnRenamed("col3", "ticket_class")
    .withColumnRenamed("col4", "ticket_type")
    .withColumnRenamed("col5", "booking_channel")
    .withColumnRenamed("col6", "ticket_price")
    .withColumnRenamed("col7", "passenger_count")
    .withColumnRenamed("col8", "currency")
)


# ------------------------------------------------------------
# 17. RENAME PAYMENTS COLUMNS
# ------------------------------------------------------------

payments = (
    payments
    .withColumnRenamed("col0", "payment_id")
    .withColumnRenamed("col1", "booking_id")
    .withColumnRenamed("col2", "payment_date")
    .withColumnRenamed("col3", "payment_method")
    .withColumnRenamed("col4", "amount")
    .withColumnRenamed("col5", "currency")
    .withColumnRenamed("col6", "payment_status")
)


# ------------------------------------------------------------
# 18. RENAME DISRUPTIONS COLUMNS
# ------------------------------------------------------------

disruptions = (
    disruptions
    .withColumnRenamed("col0", "disruption_id")
    .withColumnRenamed("col1", "journey_id")
    .withColumnRenamed("col2", "disruption_reason")
    .withColumnRenamed("col3", "severity")
    .withColumnRenamed("col4", "delay_minutes")
    .withColumnRenamed("col5", "resolution_status")
)


# ------------------------------------------------------------
# 19. RENAME REFUNDS COLUMNS
# ------------------------------------------------------------

refunds = (
    refunds
    .withColumnRenamed("col0", "refund_id")
    .withColumnRenamed("col1", "booking_id")
    .withColumnRenamed("col2", "request_date")
    .withColumnRenamed("col3", "refund_reason")
    .withColumnRenamed("col4", "refund_amount")
    .withColumnRenamed("col5", "currency")
    .withColumnRenamed("col6", "refund_status")
)


# ------------------------------------------------------------
# 20. VERIFY RENAMED SCHEMAS
# ------------------------------------------------------------

print("Renaming completed successfully")

print("Bookings schema:")
bookings.printSchema()

print("Journeys schema:")
journeys.printSchema()

print("Stations schema:")
stations.printSchema()



# ============================================================
# PART 3 - CLEAN AND STANDARDISE THE DATA
# ============================================================

from pyspark.sql.functions import (
    col,
    trim,
    upper,
    initcap,
    to_date
)


# ------------------------------------------------------------
# 21. REMOVE EXACT DUPLICATES
# ------------------------------------------------------------

stations = stations.dropDuplicates()
trains = trains.dropDuplicates()
journeys = journeys.dropDuplicates()
bookings = bookings.dropDuplicates()
payments = payments.dropDuplicates()
disruptions = disruptions.dropDuplicates()
refunds = refunds.dropDuplicates()


# ------------------------------------------------------------
# 22. TRIM STRING VALUES
# ------------------------------------------------------------
# Removes accidental spaces at the beginning or end
# of text values.

def trim_string_columns(df):
    for field in df.schema.fields:
        if field.dataType.simpleString() == "string":
            df = df.withColumn(field.name, trim(col(field.name)))
    return df


stations = trim_string_columns(stations)
trains = trim_string_columns(trains)
journeys = trim_string_columns(journeys)
bookings = trim_string_columns(bookings)
payments = trim_string_columns(payments)
disruptions = trim_string_columns(disruptions)
refunds = trim_string_columns(refunds)


# ------------------------------------------------------------
# 23. STANDARDISE STATIONS DATA
# ------------------------------------------------------------

stations = (
    stations
    .withColumn("station_name", initcap(col("station_name")))
    .withColumn("city", initcap(col("city")))
    .withColumn("country", initcap(col("country")))
)


# ------------------------------------------------------------
# 24. STANDARDISE TRAINS DATA TYPES
# ------------------------------------------------------------

trains = (
    trains
    .withColumn("capacity", col("capacity").cast("int"))
    .withColumn("service_start_year", col("service_start_year").cast("int"))
)


# ------------------------------------------------------------
# 25. STANDARDISE JOURNEYS DATA TYPES
# ------------------------------------------------------------

journeys = (
    journeys
    .withColumn("journey_date", to_date(col("journey_date"), "yyyy-MM-dd"))
    .withColumn("delay_minutes", col("delay_minutes").cast("int"))
)


# ------------------------------------------------------------
# 26. STANDARDISE BOOKINGS DATA TYPES
# ------------------------------------------------------------

bookings = (
    bookings
    .withColumn("booking_date", to_date(col("booking_date"), "yyyy-MM-dd"))
    .withColumn("ticket_price", col("ticket_price").cast("double"))
    .withColumn("passenger_count", col("passenger_count").cast("int"))
    .withColumn("currency", upper(col("currency")))
)


# ------------------------------------------------------------
# 27. STANDARDISE PAYMENTS DATA TYPES
# ------------------------------------------------------------

payments = (
    payments
    .withColumn("payment_date", to_date(col("payment_date"), "yyyy-MM-dd"))
    .withColumn("amount", col("amount").cast("double"))
    .withColumn("currency", upper(col("currency")))
)


# ------------------------------------------------------------
# 28. STANDARDISE DISRUPTIONS DATA TYPES
# ------------------------------------------------------------

disruptions = (
    disruptions
    .withColumn("delay_minutes", col("delay_minutes").cast("int"))
)


# ------------------------------------------------------------
# 29. STANDARDISE REFUNDS DATA TYPES
# ------------------------------------------------------------

refunds = (
    refunds
    .withColumn("request_date", to_date(col("request_date"), "yyyy-MM-dd"))
    .withColumn("refund_amount", col("refund_amount").cast("double"))
    .withColumn("currency", upper(col("currency")))
)


# ------------------------------------------------------------
# 30. REMOVE ROWS WITH MISSING PRIMARY KEYS
# ------------------------------------------------------------
# Primary keys are critical. Records without them should not
# enter the clean Silver layer.

stations = stations.filter(col("station_id").isNotNull())
trains = trains.filter(col("train_id").isNotNull())
journeys = journeys.filter(col("journey_id").isNotNull())
bookings = bookings.filter(col("booking_id").isNotNull())
payments = payments.filter(col("payment_id").isNotNull())
disruptions = disruptions.filter(col("disruption_id").isNotNull())
refunds = refunds.filter(col("refund_id").isNotNull())


# ------------------------------------------------------------
# 31. DEFENSIVE PRIMARY-KEY DEDUPLICATION
# ------------------------------------------------------------

stations = stations.dropDuplicates(["station_id"])
trains = trains.dropDuplicates(["train_id"])
journeys = journeys.dropDuplicates(["journey_id"])
bookings = bookings.dropDuplicates(["booking_id"])
payments = payments.dropDuplicates(["payment_id"])
disruptions = disruptions.dropDuplicates(["disruption_id"])
refunds = refunds.dropDuplicates(["refund_id"])


# ------------------------------------------------------------
# 32. CHECK RECORD COUNTS AFTER CLEANING
# ------------------------------------------------------------

print("Cleaning and standardisation completed")

print("Stations after cleaning:", stations.count())
print("Trains after cleaning:", trains.count())
print("Journeys after cleaning:", journeys.count())
print("Bookings after cleaning:", bookings.count())
print("Payments after cleaning:", payments.count())
print("Disruptions after cleaning:", disruptions.count())
print("Refunds after cleaning:", refunds.count())




# ============================================================
# PART 4 - VALIDATE RELATIONSHIPS BETWEEN TABLES
# ============================================================


# ------------------------------------------------------------
# 33. VALIDATE BOOKINGS -> JOURNEYS
# ------------------------------------------------------------

# Keep only bookings that reference a valid journey_id.
valid_journeys = journeys.select("journey_id").distinct()

bookings = bookings.join(
    valid_journeys,
    on="journey_id",
    how="inner"
)


# ------------------------------------------------------------
# 34. VALIDATE PAYMENTS -> BOOKINGS
# ------------------------------------------------------------

# Keep only payments that reference an existing booking_id.
valid_bookings = bookings.select("booking_id").distinct()

payments = payments.join(
    valid_bookings,
    on="booking_id",
    how="inner"
)


# ------------------------------------------------------------
# 35. VALIDATE REFUNDS -> BOOKINGS
# ------------------------------------------------------------

refunds = refunds.join(
    valid_bookings,
    on="booking_id",
    how="inner"
)


# ------------------------------------------------------------
# 36. VALIDATE DISRUPTIONS -> JOURNEYS
# ------------------------------------------------------------

disruptions = disruptions.join(
    valid_journeys,
    on="journey_id",
    how="inner"
)


# ------------------------------------------------------------
# 37. VALIDATE JOURNEYS -> TRAINS
# ------------------------------------------------------------

valid_trains = trains.select("train_id").distinct()

journeys = journeys.join(
    valid_trains,
    on="train_id",
    how="inner"
)


# ------------------------------------------------------------
# 38. VALIDATE JOURNEY STATIONS
# ------------------------------------------------------------

valid_stations = stations.select("station_id").distinct()

journeys = (
    journeys
    .join(
        valid_stations.withColumnRenamed(
            "station_id",
            "origin_station_id"
        ),
        on="origin_station_id",
        how="inner"
    )
    .join(
        valid_stations.withColumnRenamed(
            "station_id",
            "destination_station_id"
        ),
        on="destination_station_id",
        how="inner"
    )
)


# ------------------------------------------------------------
# 39. VERIFY COUNTS AFTER RELATIONSHIP VALIDATION
# ------------------------------------------------------------

print("Relationship validation completed")

print("Stations:", stations.count())
print("Trains:", trains.count())
print("Journeys:", journeys.count())
print("Bookings:", bookings.count())
print("Payments:", payments.count())
print("Disruptions:", disruptions.count())
print("Refunds:", refunds.count())




# ============================================================
# PART 5 - WRITE CLEAN DATA TO THE SILVER LAYER
# ============================================================


# ------------------------------------------------------------
# 40. DEFINE THE SILVER S3 BASE PATH
# ------------------------------------------------------------

silver_base_path = "s3://eurostar-data-platform-sayed/silver/"


# ------------------------------------------------------------
# 41. WRITE STATIONS TO SILVER
# ------------------------------------------------------------

stations.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(silver_base_path + "stations/")


# ------------------------------------------------------------
# 42. WRITE TRAINS TO SILVER
# ------------------------------------------------------------

trains.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(silver_base_path + "trains/")


# ------------------------------------------------------------
# 43. WRITE JOURNEYS TO SILVER
# ------------------------------------------------------------

journeys.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(silver_base_path + "journeys/")


# ------------------------------------------------------------
# 44. WRITE BOOKINGS TO SILVER
# ------------------------------------------------------------

bookings.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(silver_base_path + "bookings/")


# ------------------------------------------------------------
# 45. WRITE PAYMENTS TO SILVER
# ------------------------------------------------------------

payments.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(silver_base_path + "payments/")


# ------------------------------------------------------------
# 46. WRITE DISRUPTIONS TO SILVER
# ------------------------------------------------------------

disruptions.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(silver_base_path + "disruptions/")


# ------------------------------------------------------------
# 47. WRITE REFUNDS TO SILVER
# ------------------------------------------------------------

refunds.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(silver_base_path + "refunds/")


# ------------------------------------------------------------
# 48. CONFIRM SILVER WRITE COMPLETED
# ------------------------------------------------------------

print("All seven cleaned tables were written to the Silver layer successfully.")


# ------------------------------------------------------------
# 49. COMPLETE THE GLUE JOB
# ------------------------------------------------------------

job.commit()
