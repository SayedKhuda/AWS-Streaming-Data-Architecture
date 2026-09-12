
#############    PART 1
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# define the Database
silver_db = "eurostar_silver_db"

# Read Bookings
bookings = glueContext.create_dynamic_frame.from_catalog(
    database = silver_db,
    table_name = "bookings"
).toDF()


#Read Journeys
journeys = glueContext.create_dynamic_frame.from_catalog(
    database = silver_db,
    table_name = "journeys"
).toDF()



#Read stations
stations = glueContext.create_dynamic_frame.from_catalog(
    database = silver_db,
    table_name = "stations"
).toDF()


#Read trains
trains = glueContext.create_dynamic_frame.from_catalog(
    database=silver_db,
    table_name="trains"
).toDF()



# Read payments
payments = glueContext.create_dynamic_frame.from_catalog(
    database=silver_db,
    table_name="payments"
).toDF()


#Read refunds
refunds = glueContext.create_dynamic_frame.from_catalog(
    database=silver_db,
    table_name="refunds"
).toDF()


#Read disruptions
disruptions = glueContext.create_dynamic_frame.from_catalog(
    database=silver_db,
    table_name="disruptions"
).toDF()


#varify that all silver tables were loaded
print("Silver tables loaded successfully")


#check record counts
print("Bookings:", bookings.count())
print("Journeys:", journeys.count())
print("Stations:", stations.count())
print("Trains:", trains.count())
print("Payments:", payments.count())
print("Refunds:", refunds.count())
print("Disruptions:", disruptions.count())



# ============================================================
# PART 2 - CREATE GOLD DIMENSION TABLES
# ============================================================

from pyspark.sql.functions import col


# Create Dimention Tables

dim_station = stations.select(
    "station_id",
    "station_name",
    "city",
    "country"
).dropDuplicates(["station_id"])



dim_train = trains.select(
    "train_id",
    "train_number",
    "train_model",
    "capacity",
    "service_start_year"
).dropDuplicates(["train_id"])


# ------------------------------------------------------------
#  VERIFY DIMENSION TABLES
# ------------------------------------------------------------

print("Gold dimension tables created successfully")

print("dim_station rows:", dim_station.count())
print("dim_train rows:", dim_train.count())

print("dim_station schema:")
dim_station.printSchema()

print("dim_train schema:")
dim_train.printSchema()

# ============================================================
# PART 3 - CREATE DATE DIMENSION
# ============================================================

from pyspark.sql.functions import (
    col,
    year,
    month,
    quarter,
    dayofmonth,
    dayofweek,
    date_format,
    lit
)


# ------------------------------------------------------------
# COLLECT ALL IMPORTANT BUSINESS DATES 
# ------------------------------------------------------------

booking_dates = bookings.select(
    col("booking_date").alias("date")
)

journey_dates = journeys.select(
    col("journey_date").alias("date")
)

payment_dates = payments.select(
    col("payment_date").alias("date")
)

refund_dates = refunds.select(
    col("request_date").alias("date")
)


# ------------------------------------------------------------
# COMBINE ALL DATES
# ------------------------------------------------------------

all_dates = (
    booking_dates
    .union(journey_dates)
    .union(payment_dates)
    .union(refund_dates)
    .filter(col("date").isNotNull())
    .dropDuplicates(["date"])
)


# ------------------------------------------------------------
# 18. CREATE DATE DIMENSION ATTRIBUTES
# ------------------------------------------------------------

dim_date = (
    all_dates

    .withColumn("date_key", date_format(col("date"), "yyyyMMdd").cast("int"))

    .withColumn("day", dayofmonth(col("date")))

    .withColumn("month", month(col("date")))

    .withColumn("month_name", date_format(col("date"), "MMMM"))

    .withColumn("quarter", quarter(col("date")))

    .withColumn("year", year(col("date")))

    .withColumn("day_of_week_number", dayofweek(col("date")))

    .withColumn("day_name", date_format(col("date"), "EEEE"))
)


# ------------------------------------------------------------
# SELECT FINAL DATE DIMENSION COLUMNS
# ------------------------------------------------------------

dim_date = dim_date.select(
    "date_key",
    "date",
    "day",
    "month",
    "month_name",
    "quarter",
    "year",
    "day_of_week_number",
    "day_name"
)


# ------------------------------------------------------------
# VERIFY DATE DIMENSION
# ------------------------------------------------------------

print("dim_date created successfully")

print("dim_date rows:", dim_date.count())

dim_date.printSchema()





# ============================================================
# PART 4 - CREATE FACT JOURNEYS
# ============================================================

from pyspark.sql.functions import (
    col,
    date_format,
    when,
    lit
)


# ------------------------------------------------------------
# CREATE JOURNEY DATE KEY
# ------------------------------------------------------------

fact_journeys = journeys.withColumn(
    "journey_date_key",
    date_format(col("journey_date"), "yyyyMMdd").cast("int")
)


# ------------------------------------------------------------
# CREATE JOURNEY PERFORMANCE FLAGS
# ------------------------------------------------------------

fact_journeys = (
    fact_journeys

    .withColumn(
        "is_on_time",
        when(col("journey_status") == "On Time", 1).otherwise(0)
    )

    .withColumn(
        "is_delayed",
        when(col("journey_status") == "Delayed", 1).otherwise(0)
    )

    .withColumn(
        "is_cancelled",
        when(col("journey_status") == "Cancelled", 1).otherwise(0)
    )
)


# ------------------------------------------------------------
# ADD JOURNEY COUNT MEASURE
# ------------------------------------------------------------

fact_journeys = fact_journeys.withColumn(
    "journey_count",
    lit(1)
)


# ------------------------------------------------------------
# SELECT FINAL FACT JOURNEY COLUMNS
# ------------------------------------------------------------

fact_journeys = fact_journeys.select(

    # Primary business key
    "journey_id",

    # Dimension keys
    "journey_date_key",
    "train_id",
    "origin_station_id",
    "destination_station_id",

    # Journey time information
    "journey_date",
    "scheduled_departure",
    "scheduled_arrival",
    "actual_arrival",

    # Journey performance attributes
    "journey_status",

    # Numerical measure
    "delay_minutes",

    # Analytical measures / flags
    "journey_count",
    "is_on_time",
    "is_delayed",
    "is_cancelled"
)


# ------------------------------------------------------------
#  VERIFY FACT JOURNEYS
# ------------------------------------------------------------

print("fact_journeys created successfully")

print(
    "fact_journeys rows:",
    fact_journeys.count()
)

fact_journeys.printSchema()




# ============================================================
# PART 5 - CREATE FACT BOOKINGS
# ============================================================

from pyspark.sql.functions import (
    col,
    date_format,
    lit
)


# ------------------------------------------------------------
# CREATE BOOKING DATE KEY
# ------------------------------------------------------------

fact_bookings = bookings.withColumn(
    "booking_date_key",
    date_format(col("booking_date"), "yyyyMMdd").cast("int")
)


# ------------------------------------------------------------
# ADD BOOKING COUNT MEASURE
# ------------------------------------------------------------


fact_bookings = fact_bookings.withColumn(
    "booking_count",
    lit(1)
)


# ------------------------------------------------------------
# CALCULATE BOOKING VALUE
# ------------------------------------------------------------
# ticket_price is the price per booking in our dataset.
#
# We keep ticket_price as a numeric measure so that Athena
# and QuickSight can calculate:
# SUM(ticket_price)
# AVG(ticket_price)
# ------------------------------------------------------------

fact_bookings = fact_bookings.withColumn(
    "ticket_value",
    col("ticket_price").cast("double")
)


# ------------------------------------------------------------
# SELECT FINAL FACT BOOKING COLUMNS
# ------------------------------------------------------------

fact_bookings = fact_bookings.select(

    # Business keys
    "booking_id",
    "journey_id",

    # Date dimension key
    "booking_date_key",

    # Original business date
    "booking_date",

    # Booking attributes
    "ticket_class",
    "ticket_type",
    "booking_channel",
    "currency",

    # Measures
    "ticket_price",
    "ticket_value",
    "passenger_count",
    "booking_count"
)


# ------------------------------------------------------------
# VERIFY FACT BOOKINGS
# ------------------------------------------------------------

print("fact_bookings created successfully")

print(
    "fact_bookings rows:",
    fact_bookings.count()
)

fact_bookings.printSchema()




# ============================================================
# PART 6 - CREATE REMAINING FACT TABLES
# ============================================================

from pyspark.sql.functions import (
    col,
    date_format,
    lit,
    when
)


# ------------------------------------------------------------
# CREATE FACT PAYMENTS
# ------------------------------------------------------------

fact_payments = (
    payments
    .withColumn(
        "payment_date_key",
        date_format(col("payment_date"), "yyyyMMdd").cast("int")
    )
    .withColumn(
        "payment_count",
        lit(1)
    )
    .withColumn(
        "is_completed",
        when(col("payment_status") == "Completed", 1).otherwise(0)
    )
    .withColumn(
        "is_failed",
        when(col("payment_status") == "Failed", 1).otherwise(0)
    )
    .withColumn(
        "is_refunded",
        when(col("payment_status") == "Refunded", 1).otherwise(0)
    )
)

fact_payments = fact_payments.select(
    "payment_id",
    "booking_id",
    "payment_date_key",
    "payment_date",
    "payment_method",
    "payment_status",
    "currency",
    "amount",
    "payment_count",
    "is_completed",
    "is_failed",
    "is_refunded"
)


# ------------------------------------------------------------
# CREATE FACT REFUNDS
# ------------------------------------------------------------

fact_refunds = (
    refunds
    .withColumn(
        "refund_date_key",
        date_format(col("request_date"), "yyyyMMdd").cast("int")
    )
    .withColumn(
        "refund_count",
        lit(1)
    )
)

fact_refunds = fact_refunds.select(
    "refund_id",
    "booking_id",
    "refund_date_key",
    "request_date",
    "refund_reason",
    "refund_status",
    "currency",
    "refund_amount",
    "refund_count"
)


# ------------------------------------------------------------
# CREATE FACT DISRUPTIONS
# ------------------------------------------------------------

fact_disruptions = (
    disruptions
    .withColumn(
        "disruption_count",
        lit(1)
    )
)

fact_disruptions = fact_disruptions.select(
    "disruption_id",
    "journey_id",
    "disruption_reason",
    "severity",
    "resolution_status",
    "delay_minutes",
    "disruption_count"
)


# ------------------------------------------------------------
# VERIFY REMAINING FACT TABLES
# ------------------------------------------------------------

print("Remaining Gold fact tables created successfully")

print("fact_payments rows:", fact_payments.count())
print("fact_refunds rows:", fact_refunds.count())
print("fact_disruptions rows:", fact_disruptions.count())

print("fact_payments schema:")
fact_payments.printSchema()

print("fact_refunds schema:")
fact_refunds.printSchema()

print("fact_disruptions schema:")
fact_disruptions.printSchema()



# ============================================================
# PART 7 - WRITE GOLD TABLES TO S3
# ============================================================

gold_base_path = "s3://eurostar-data-platform-sayed/gold/"


# ------------------------------------------------------------
# WRITE DIMENSION TABLES
# ------------------------------------------------------------

dim_station.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(gold_base_path + "dim_station/")


dim_train.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(gold_base_path + "dim_train/")


dim_date.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(gold_base_path + "dim_date/")


# ------------------------------------------------------------
# 37. WRITE FACT TABLES
# ------------------------------------------------------------

fact_journeys.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(gold_base_path + "fact_journeys/")


fact_bookings.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(gold_base_path + "fact_bookings/")


fact_payments.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(gold_base_path + "fact_payments/")


fact_refunds.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(gold_base_path + "fact_refunds/")


fact_disruptions.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(gold_base_path + "fact_disruptions/")



print("All Gold dimension and fact tables were written successfully.")

job.commit()
