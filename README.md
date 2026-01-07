# Project Architecture:

![alt text](architecture_diagram.png)
The following diagram illustrates the "End-to-End" Big Data pipeline currently implemented up to Step 3 (HDFS Storage).

# Architecture Review

Component Details

1.  Traffic Generator (traffic-generator)
    Role: Simulates IoT sensors from a Smart City.
    Behavior: continuously generates random but realistic traffic events (vehicle count, speed, occupancy).
    Output: JSON messages sent to Kafka.

2.  Kafka Cluster (kafka, zookeeper)
    Role: Message broker buffer.
    Topic: traffic-events (3 partitions).
    Function: Decouples the high-speed generator from the storage layer.

3.  Ingest Service (ingest-service)
    Role: Bridge between Streaming and Batch storage.
    Behavior: Consumes messages from Kafka, buffers them (batch size: 10), and writes files to HDFS.
    Format: JSON files stored in /data/raw/traffic/date=YYYY-MM-DD/zone=ZONE/.

4.  HDFS (namenode, datanode1)
    Role: Distributed Data Lake (Raw Zone).
    Structure: Hierarchical directory structure partitioned by Date and Zone.

## Raw Data Format (JSON)

Each event stored in `/data/raw/traffic` follows this structure:

```json
{
  "sensor_id": "S001",
  "road_id": "R005",
  "road_type": "Highway",
  "zone": "Industrial",
  "vehicle_count": 45,
  "average_speed": 82,
  "occupancy_rate": 35.5,
  "event_time": "2026-01-02T14:30:00.000000"
}
```

## Integration with Part 4 (Data Processing)

The work achieved so far (Steps 1-3) provides the foundational **Raw Data Layer** of the Data Lake.
Part 4 (Spark Processing) will build upon this by:

1.  **Reading**: Accessing the raw JSON files accumulated in HDFS (`/data/raw/traffic`).
2.  **Processing**: Using Apache Spark to perform batch aggregations (e.g., calculating average traffic per zone) that serve as business indicators.
3.  **Transformation**: Converting this raw, voluminous data into optimized insights (Parquet format) ready for analysis.

traffic-generator:

![alt text](images/image.png)

ingest service:
![alt text](images/image-1.png)

access hdfs:

exemple
docker exec namenode hdfs dfs -cat /data/raw/traffic/date=2026-01-02/zone=Residential-North/events_1767361749298.json

```json
{
  "sensor_id": "S009",
  "road_id": "R002",
  "road_type": "Highway",
  "zone": "Residential-North",
  "vehicle_count": 47,
  "average_speed": 76,
  "occupancy_rate": 47.34,
  "event_time": "2026-01-02T13:48:26.456111"
},
{
  "sensor_id": "S009",
  "road_id": "R002",
  "road_type": "Highway",
  "zone": "Residential-North",
  "vehicle_count": 51,
  "average_speed": 73,
  "occupancy_rate": 51.74,
  "event_time": "2026-01-02T13:48:54.851994"
}
```

type of generated traffic events:

```python
ZONES = ["Downtown", "Industrial", "Residential-North", "Residential-South", "Suburbs"]

ROAD_TYPES = ["Highway", "Avenue", "Street"]
```

## Step 4: Data Processing with Apache Spark

### Overview
Reads raw JSON from HDFS, performs batch analytics, saves processed results as Parquet.

### Analysis Performed

**KPI 1: Traffic by Zone**
- Groups by zone
- Calculates: avg vehicle count, event count
- Identifies busiest zones

**KPI 2: Speed by Road Type**
- Groups by road type (Highway/Avenue/Street)
- Calculates: avg speed
- Compares road efficiency

**KPI 3: Congestion Detection**
- Flags congestion when: speed < 40 km/h OR occupancy > 70%
- Outputs: congested events with location/time

**KPI 4: Hourly Traffic Patterns**
- Groups by hour (0-23)
- Calculates: avg vehicles, avg speed per hour
- Reveals peak hours

**KPI 5: Critical Zones**
- Counts congestion events per zone
- Ranks zones by congestion frequency

### Output Paths (HDFS)
```
hdfs://namenode:8020/data/processed/traffic/traffic_by_zone
hdfs://namenode:8020/data/processed/traffic/speed_by_road
hdfs://namenode:8020/data/processed/traffic/congestion_alerts
hdfs://namenode:8020/data/processed/traffic/hourly_stats
hdfs://namenode:8020/data/processed/traffic/critical_zones
```

### Run Processing Job
```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/spark_processor.py
```

---

## Step 5: Load to PostgreSQL

### Overview
Reads Parquet from HDFS, loads to relational database for Grafana queries.

### PostgreSQL Tables & Schemas

**traffic_by_zone**
```sql
zone (STRING)
avg_vehicles (DOUBLE)
event_count (LONG)
```

**speed_by_road**
```sql
road_type (STRING)
avg_speed (DOUBLE)
```

**congestion_alerts**
```sql
zone (STRING)
road_type (STRING)
average_speed (DOUBLE)
occupancy_rate (DOUBLE)
event_time (TIMESTAMP)
```

**hourly_stats**
```sql
hour (INTEGER)
avg_vehicles (DOUBLE)
avg_speed (DOUBLE)
```

**critical_zones**
```sql
zone (STRING)
congestion_count (LONG)
```

### Run Loader Job
```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark-jars/postgresql-42.6.0.jar \
  /opt/spark-apps/load_to_postgres.py
```

---

## Step 6: Grafana Visualization

### Dashboard: Smart City Traffic Monitoring

**Access:** http://localhost:3000 (admin/admin)

### Panels Created

**1. Traffic by Hour**
- **Query:** `SELECT hour, AVG(avg_vehicles) FROM hourly_stats GROUP BY hour ORDER BY hour`
- **Chart:** Bar chart
- **Insight:** Identifies rush hours (8-9 AM, 5-7 PM spikes)
- **Value:** Helps plan traffic light timing, public transport schedules

**2. Average Speed by Road Type**
- **Query:** `SELECT road_type, avg_speed FROM speed_by_road ORDER BY avg_speed DESC`
- **Chart:** Bar chart
- **Insight:** Highways fastest (~80 km/h), Streets slowest (~45 km/h)
- **Value:** Validates road design expectations, spots anomalies

**3. Traffic by Zone**
- **Query:** `SELECT zone, avg_vehicles FROM traffic_by_zone ORDER BY avg_vehicles DESC`
- **Chart:** Bar chart
- **Insight:** Ranks zones by activity (Downtown busiest)
- **Value:** Guides infrastructure investment priorities

**4. Critical Zones (Congestion Hotspots)**
- **Query:** `SELECT zone, congestion_count FROM critical_zones ORDER BY congestion_count DESC LIMIT 5`
- **Chart:** Table
- **Insight:** Shows which zones have most congestion events
- **Value:** Targets intervention areas (add lanes, optimize signals)

**5. Real-time Congestion Alerts** (Optional)
- **Query:** `SELECT zone, road_type, average_speed, event_time FROM congestion_alerts ORDER BY event_time DESC LIMIT 10`
- **Chart:** Table
- **Insight:** Latest congestion incidents
- **Value:** Enables real-time traffic management response

### Dashboard Added Value

**For City Planners:**
- Data-driven decisions on infrastructure
- Identify chronic problem areas
- Measure intervention effectiveness

**For Traffic Management:**
- Real-time congestion monitoring
- Predictive maintenance scheduling
- Emergency routing optimization

**For Citizens:**
- Improved commute times
- Reduced emissions (less idling)
- Better urban quality of life

## Step 7: Airflow Orchestration

### Overview
Automates the entire data processing pipeline on a schedule. Ensures Spark jobs run reliably with retries and monitoring.

### DAG: `traffic_data_pipeline`

**Schedule:** Every 3 minutes  
**Owner:** data-engineer  
**Retries:** 3 attempts (2-minute delay between retries)

### Pipeline Tasks

**Task 1: spark_process_traffic**
- Triggers Spark job in spark-master container
- Reads raw JSON from HDFS
- Calculates all 5 KPIs (zone traffic, speed by road, congestion, hourly stats, critical zones)
- Saves Parquet to `/data/processed/traffic/`

**Task 2: load_to_postgres**
- Triggers PostgreSQL loader job
- Reads all Parquet files from Step 4
- Loads data into 5 PostgreSQL tables
- Requires PostgreSQL JDBC jar

**Task 3: verify_data_loaded**
- Checks PostgreSQL connection
- Verifies `traffic_by_zone` table has data
- Fails pipeline if no records found
- Prints success message with row count

### Task Dependencies
```
spark_process_traffic → load_to_postgres → verify_data_loaded
```

### Access & Monitor

**Web UI:** http://localhost:8085 (admin/admin)

**Trigger Manually:**
- Click DAG name → Click "Play" button (top-right)
- Monitor logs in real-time per task

**View Logs:**
- Click task box → Click "Log" tab
- See Spark output, PostgreSQL inserts, verification results

### What It Automates

**Every Hour:**
1. Processes last hour's raw traffic data
2. Updates analytics tables in PostgreSQL
3. Makes fresh data available to Grafana
4. Sends alerts if pipeline fails

**Value:**
- No manual intervention needed
- Consistent data freshness
- Automatic retry on failures
- Full audit trail in logs