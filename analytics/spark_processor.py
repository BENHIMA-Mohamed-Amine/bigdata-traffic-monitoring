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

# HDFS - Changed port 9000 → 8020
df = spark.read.json("hdfs://namenode:8020/data/raw/traffic/*/*/*.json")

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
# SAVE RESULTS TO HDFS
# ==========================================

# Changed port 9000 → 8020
traffic_by_zone.write.mode("overwrite").parquet(
    "hdfs://namenode:8020/data/processed/traffic/traffic_by_zone"
)
speed_by_road.write.mode("overwrite").parquet(
    "hdfs://namenode:8020/data/processed/traffic/speed_by_road"
)
congestion_alerts.write.mode("overwrite").parquet(
    "hdfs://namenode:8020/data/processed/traffic/congestion_alerts"
)
hourly_traffic.write.mode("overwrite").parquet(
    "hdfs://namenode:8020/data/processed/traffic/hourly_stats"
)
critical_zones.write.mode("overwrite").parquet(
    "hdfs://namenode:8020/data/processed/traffic/critical_zones"
)

print(
    "\n✅ Processing Complete! Files saved to HDFS at hdfs://namenode:8020/data/processed/traffic/"
)
spark.stop()
