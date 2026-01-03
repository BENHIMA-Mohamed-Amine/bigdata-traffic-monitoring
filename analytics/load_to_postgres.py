from pyspark.sql import SparkSession

# ==========================================
# CONFIGURATION - SWITCH BETWEEN LOCAL/DOCKER
# ==========================================

# LOCAL TESTING (running on your machine)
USE_LOCAL = False

if USE_LOCAL:
    # Local configuration
    spark = (
        SparkSession.builder.appName("LoadToPostgres")
        .config("spark.jars", "jars/postgresql-42.6.0.jar")
        .master("local[*]")
        .getOrCreate()
    )

    jdbc_url = "jdbc:postgresql://localhost:5432/traffic_db"
    output_dir = "analytics/output"
else:
    # Docker configuration
    spark = (
        SparkSession.builder.appName("LoadToPostgres")
        .config("spark.jars", "/opt/spark-jars/postgresql-42.6.0.jar")
        .master("local[*]")
        .getOrCreate()
    )

    jdbc_url = "jdbc:postgresql://postgres:5432/traffic_db"
    output_dir = "/opt/spark-apps/output"

# PostgreSQL connection properties
connection_properties = {
    "user": "traffic_user",
    "password": "traffic_pass",
    "driver": "org.postgresql.Driver",
}

print(f"📍 Using output directory: {output_dir}")
print(f"📍 Connecting to: {jdbc_url}")

# ==========================================
# READ PARQUET FILES
# ==========================================

print("📦 Reading Parquet files...")

traffic_by_zone = spark.read.parquet(f"{output_dir}/traffic_by_zone")
speed_by_road = spark.read.parquet(f"{output_dir}/speed_by_road")
congestion_alerts = spark.read.parquet(f"{output_dir}/congestion_alerts")
hourly_stats = spark.read.parquet(f"{output_dir}/hourly_stats")
critical_zones = spark.read.parquet(f"{output_dir}/critical_zones")

# ==========================================
# WRITE TO POSTGRESQL
# ==========================================

print("💾 Writing to PostgreSQL...")

traffic_by_zone.write.jdbc(
    jdbc_url, "traffic_by_zone", mode="overwrite", properties=connection_properties
)
speed_by_road.write.jdbc(
    jdbc_url, "speed_by_road", mode="overwrite", properties=connection_properties
)
congestion_alerts.write.jdbc(
    jdbc_url, "congestion_alerts", mode="overwrite", properties=connection_properties
)
hourly_stats.write.jdbc(
    jdbc_url, "hourly_stats", mode="overwrite", properties=connection_properties
)
critical_zones.write.jdbc(
    jdbc_url, "critical_zones", mode="overwrite", properties=connection_properties
)

print("✅ Data loaded to PostgreSQL!")
print(
    "📊 Tables created: traffic_by_zone, speed_by_road, congestion_alerts, hourly_stats"
)

spark.stop()
