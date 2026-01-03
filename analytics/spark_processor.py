from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, hour, when
import os

# Get absolute path to project root
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print(f"📁 Project Root: {project_root}")

# Initialize Spark Session
spark = SparkSession.builder.appName("TrafficAnalysis").master("local[*]").getOrCreate()

# ==========================================
# READ DATA
# ==========================================

# LOCAL FILE (for testing)
# input_file = os.path.join(project_root, "analytics", "dummy_traffic.json")
# df = spark.read.option("multiLine", "true").json(input_file)

# DOCKER (if running in container, uncomment)
df = spark.read.option("multiLine", "true").json("/opt/spark-apps/dummy_traffic.json")

# HDFS (uncomment when ready to integrate)
# df = spark.read.json("hdfs://namenode:9000/data/raw/traffic/*/*/*.json")

print("📊 Raw Data:")
df.show()

# ==========================================
# KPI 1: Average Traffic per Zone
# ==========================================
traffic_by_zone = (
    df.groupBy("zone")
    .agg(avg("vehicle_count").alias("avg_vehicles"), count("*").alias("event_count"))
    .orderBy(col("avg_vehicles").desc())
)

print("\n🚗 Average Traffic by Zone:")
traffic_by_zone.show()

# ==========================================
# KPI 2: Average Speed per Road Type
# ==========================================
speed_by_road = (
    df.groupBy("road_type")
    .agg(avg("average_speed").alias("avg_speed"))
    .orderBy(col("avg_speed").desc())
)

print("\n⚡ Average Speed by Road Type:")
speed_by_road.show()

# ==========================================
# KPI 3: Congestion Detection
# ==========================================
congestion_df = df.withColumn(
    "is_congested",
    when((col("average_speed") < 40) | (col("occupancy_rate") > 70), True).otherwise(
        False
    ),
)

congestion_alerts = congestion_df.filter(col("is_congested") == True).select(
    "zone", "road_type", "average_speed", "occupancy_rate", "event_time"
)

print("\n🚨 Congestion Alerts:")
congestion_alerts.show()

# ==========================================
# KPI 4: Peak Hours Analysis
# ==========================================
hourly_traffic = (
    df.withColumn("hour", hour(col("event_time")))
    .groupBy("hour")
    .agg(
        avg("vehicle_count").alias("avg_vehicles"),
        avg("average_speed").alias("avg_speed"),
    )
    .orderBy("hour")
)

print("\n📈 Traffic by Hour:")
hourly_traffic.show()

# ==========================================
# KPI 5: Critical Zones (most congested)
# ==========================================
critical_zones = (
    congestion_df.groupBy("zone")
    .agg(count(when(col("is_congested") == True, 1)).alias("congestion_count"))
    .orderBy(col("congestion_count").desc())
)

print("\n⚠️ Critical Zones:")
critical_zones.show()

# ==========================================
# SAVE RESULTS
# ==========================================

# Create output directory if it doesn't exist
# ==========================================
# CONFIGURATION - CHOOSE YOUR ENVIRONMENT
# ==========================================
USE_LOCAL = False  # Set to True for local testing
USE_DOCKER = True  # Set to True for Docker
USE_HDFS = False  # Set to True for HDFS

# ==========================================
# SAVE RESULTS
# ==========================================

if USE_LOCAL:
    # LOCAL SAVE
    output_dir = os.path.join(project_root, "analytics", "output")
    os.makedirs(output_dir, exist_ok=True)

    traffic_by_zone.write.mode("overwrite").parquet(f"{output_dir}/traffic_by_zone")
    speed_by_road.write.mode("overwrite").parquet(f"{output_dir}/speed_by_road")
    congestion_alerts.write.mode("overwrite").parquet(f"{output_dir}/congestion_alerts")
    hourly_traffic.write.mode("overwrite").parquet(f"{output_dir}/hourly_stats")
    critical_zones.write.mode("overwrite").parquet(f"{output_dir}/critical_zones")

elif USE_DOCKER:
    # DOCKER SAVE
    output_dir = "/opt/spark-apps/output"

    traffic_by_zone.write.mode("overwrite").parquet(f"{output_dir}/traffic_by_zone")
    speed_by_road.write.mode("overwrite").parquet(f"{output_dir}/speed_by_road")
    congestion_alerts.write.mode("overwrite").parquet(f"{output_dir}/congestion_alerts")
    hourly_traffic.write.mode("overwrite").parquet(f"{output_dir}/hourly_stats")
    critical_zones.write.mode("overwrite").parquet(f"{output_dir}/critical_zones")

elif USE_HDFS:
    # HDFS SAVE
    traffic_by_zone.write.mode("overwrite").parquet(
        "hdfs://namenode:9000/data/processed/traffic/traffic_by_zone"
    )
    speed_by_road.write.mode("overwrite").parquet(
        "hdfs://namenode:9000/data/processed/traffic/speed_by_road"
    )
    congestion_alerts.write.mode("overwrite").parquet(
        "hdfs://namenode:9000/data/processed/traffic/congestion_alerts"
    )
    hourly_traffic.write.mode("overwrite").parquet(
        "hdfs://namenode:9000/data/processed/traffic/hourly_stats"
    )
    critical_zones.write.mode("overwrite").parquet(
        "hdfs://namenode:9000/data/processed/traffic/critical_zones"
    )

print("\n✅ Processing Complete!")

print(f"\n✅ Processing Complete! Files saved to: {output_dir}")
spark.stop()
