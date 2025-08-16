from flask import Flask, jsonify, render_template, request
import sqlite3
import paho.mqtt.client as mqtt
from datetime import datetime
import threading
from fuzzy_logic import FuzzyWateringSystem
from flask_apscheduler import APScheduler
import logging
from contextlib import closing
from pytz import timezone

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Config:
    SCHEDULER_API_ENABLED = True

# Configuration (should be moved to environment variables)
MQTT_BROKER = "103.127.134.201"
MQTT_TOPIC_SENSOR = "esp32/sensor_data"
MQTT_TOPIC_CONTROL = "esp32/watering_control"
MQTT_USERNAME = "kayvsan"
MQTT_PASSWORD = "(Malang439)"
DATABASE = "sensor_data.db"
MQTT_PORT = 1883
MQTT_KEEPALIVE = 60

# Initialize Flask and components
app = Flask(__name__)
app.config.from_object(Config)
fuzzy_system = FuzzyWateringSystem()

# Initialize MQTT Client
mqtt_client = mqtt.Client()
mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

# Initialize scheduler
scheduler = APScheduler()
scheduler.init_app(app)

def init_db():
    """Initialize the database with required tables."""
    with closing(sqlite3.connect(DATABASE)) as conn:
        with conn:
            conn.execute('''CREATE TABLE IF NOT EXISTS sensor_data
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          timestamp TEXT,
                          temperature REAL,
                          humidity REAL,
                          soil_moisture INTEGER)''')
            logger.info("Database initialized")

# MQTT Callbacks
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Connected to MQTT Broker")
        client.subscribe(MQTT_TOPIC_SENSOR)
    else:
        logger.error(f"Failed to connect to MQTT Broker with code {rc}")

def on_message(client, userdata, msg):
    try:
        if msg.topic == MQTT_TOPIC_SENSOR:
            temp, hum, soil = msg.payload.decode().split(',')
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            with closing(sqlite3.connect(DATABASE)) as conn:
                with conn:
                    conn.execute(
                        "INSERT INTO sensor_data (timestamp, temperature, humidity, soil_moisture) VALUES (?, ?, ?, ?)",
                        (timestamp, float(temp), float(hum), int(soil))
                    )
            logger.info(f"Saved sensor data: {temp}°C, {hum}%, {soil}%")
            
    except Exception as e:
        logger.error(f"Error processing MQTT message: {e}")

def mqtt_thread():
    """Run MQTT client in a background thread."""
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE)
        mqtt_client.loop_forever()
    except Exception as e:
        logger.error(f"MQTT thread error: {e}")

def proses_data():
    """Process the latest sensor data and determine watering needs."""
    try:
        with closing(sqlite3.connect(DATABASE)) as conn:
            with conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT temperature, humidity, soil_moisture FROM sensor_data ORDER BY timestamp DESC LIMIT 1"
                )
                row = cursor.fetchone()
        
        if not row:
            logger.warning("No sensor data available")
            return None, None, None, {"error": "No data available"}
        
        temp, hum, soil = row
        result = fuzzy_system.calculate_watering(soil, hum, temp)
        
        if result.get('duration_ms', 0) > 0:
            mqtt_client.publish(MQTT_TOPIC_CONTROL, f"ON,{result['duration_ms']}")
            logger.info(f"Sent pump command: ON for {result['duration_ms']}ms")
        
        return temp, hum, soil, result
    
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        return None, None, None, {"error": str(e)}

# Flask Routes
@app.route('/')
def dashboard():
    try:
        with closing(sqlite3.connect(DATABASE)) as conn:
            with conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM sensor_data ORDER BY timestamp DESC LIMIT 10")
                data = cursor.fetchall()
        return render_template('index.html', data=data)
    except Exception as e:
        logger.error(f"Dashboard error: {e}")
        return render_template('error.html', error=str(e))

@app.route('/api/latest')
def get_latest():
    temp, hum, soil, result = proses_data()
    if None in (temp, hum, soil):
        return jsonify({"error": "Failed to get sensor data"}), 500
    return jsonify({
        "sensor_data": {
            "temperature": temp,
            "humidity": hum,
            "soil_moisture": soil
        },
        "watering_recommendation": result,
        "pump_command": "ON" if result.get('duration_ms', 0) > 0 else "OFF"
    })

@app.route('/api/water/activate', methods=['POST'])
def activate_water():
    try:
        duration = int(request.json.get('duration', 30000))
        if duration <= 0:
            return jsonify({"error": "Duration must be positive"}), 400
            
        mqtt_client.publish(MQTT_TOPIC_CONTROL, f"ON,{duration}")
        return jsonify({
            "status": "success",
            "message": f"Watering command sent to device for {duration}ms"
        })
    except Exception as e:
        logger.error(f"Activate water error: {e}")
        return jsonify({"error": str(e)}), 500

def run_app():
    """Initialize and run the application."""
    init_db()
    
    # Start MQTT in background
    mqtt_thread_instance = threading.Thread(target=mqtt_thread)
    mqtt_thread_instance.daemon = True
    mqtt_thread_instance.start()
    
    # Start scheduler
    if not scheduler.running:
        scheduler.start()
        
        # Set timezone (sesuaikan dengan lokasi Anda)
        jakarta_tz = timezone('Asia/Jakarta')
        
        # Jadwalkan proses_data jam 7 pagi dan 5 sore
        scheduler.add_job(
            id='Morning Watering',
            func=proses_data,
            trigger='cron',
            hour=7,
            minute=0,
            timezone=jakarta_tz
        )
        
        scheduler.add_job(
            id='Evening Watering',
            func=proses_data,
            trigger='cron',
            hour=17,
            minute=0,
            timezone=jakarta_tz
        )
    
    # Start Flask
    app.run(host='0.0.0.0', port=5000)

if __name__ == '__main__':
    run_app()