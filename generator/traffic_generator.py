import json
import random
import time
import datetime
import uuid

from kafka import KafkaProducer

# Configuration
SENSORS_COUNT = 10
ZONES = ["Downtown", "Industrial", "Residential-North", "Residential-South", "Suburbs"]
ROAD_TYPES = ["Highway", "Avenue", "Street"]
KAFKA_TOPIC = "traffic-events"
KAFKA_BOOTSTRAP_SERVERS = ['kafka:29092']

class TrafficSensor:
    def __init__(self, sensor_id, road_id, road_type, zone):
        self.sensor_id = sensor_id
        self.road_id = road_id
        self.road_type = road_type
        self.zone = zone

    def generate_data(self, current_time):
        # Time of day
        hour = current_time.hour
        
        # values based on road type
        if self.road_type == "Highway":
            capacity = 100
            max_speed = 120
        elif self.road_type == "Avenue":
            capacity = 50
            max_speed = 60
        else:
            capacity = 20
            max_speed = 50

        # Rush hour logic (7-9 AM and 5-7 PM)
        is_rush_hour = (7 <= hour <= 9) or (17 <= hour <= 19)
        day_time = 6 <= hour <= 22

        if is_rush_hour:
             # High traffic, potential congestion
             occupancy_factor = random.uniform(0.7, 1.0)
        elif day_time:
             # Normal day traffic
             occupancy_factor = random.uniform(0.3, 0.7)
        else:
             # Night traffic
             occupancy_factor = random.uniform(0.0, 0.2)

        # Calculate vehicle count based on capacity and occupancy
        vehicle_count = int(capacity * occupancy_factor)
        
        # Calculate speed based on occupancy
        speed_factor = 1.0 - (occupancy_factor ** 1.5)
        average_speed = max(5, int(max_speed * speed_factor * random.uniform(0.9, 1.1)))

        # Adjust occupancy rate for output (0-100)
        occupancy_rate = round(occupancy_factor * 100, 2)

        event = {
            "sensor_id": self.sensor_id,
            "road_id": self.road_id,
            "road_type": self.road_type,
            "zone": self.zone,
            "vehicle_count": vehicle_count,
            "average_speed": average_speed,
            "occupancy_rate": occupancy_rate,
            "event_time": current_time.isoformat()
        }
        return event

def initialize_sensors(count):
    sensors = []
    for i in range(count):
        sensor_id = f"S{i:03d}"
        road_id = f"R{random.randint(1, 20):03d}"
        road_type = random.choice(ROAD_TYPES)
        zone = random.choice(ZONES)
        sensors.append(TrafficSensor(sensor_id, road_id, road_type, zone))
    return sensors

def main():
    print(f"Initializing Producer connecting to {KAFKA_BOOTSTRAP_SERVERS}...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        api_version=(2, 5, 0),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    sensors = initialize_sensors(SENSORS_COUNT)
    print(f"Initialized {len(sensors)} sensors. Starting generation...")
    
    try:
        while True:
            current_time = datetime.datetime.now()
            sensor = random.choice(sensors)
            event_data = sensor.generate_data(current_time)
            
            # Send to Kafka
            producer.send(KAFKA_TOPIC, value=event_data, key=sensor.sensor_id.encode('utf-8'))
            producer.flush() 
            
            print(f"Sent: {event_data['sensor_id']} - {event_data['event_time']}")
            
            time.sleep(0.5) 
            
    except KeyboardInterrupt:
        print("\nSimulation stopped.")
        producer.close()

if __name__ == "__main__":
    main()
