from flask import Flask, jsonify, render_template, request
import sqlite3
import paho.mqtt.client as mqtt
from datetime import datetime
import threading
import time
import logging
from contextlib import closing
from flask_apscheduler import APScheduler
from pytz import timezone
import signal
import sys

# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)
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

# Import fuzzy logic after app creation to avoid circular imports
from fuzzy_logic import FuzzyWateringSystem
fuzzy_system = FuzzyWateringSystem()

# Initialize MQTT Client
mqtt_client = mqtt.Client()
mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

# Initialize scheduler
scheduler = APScheduler()
scheduler.init_app(app)

def get_db_connection():
    """Get a database connection with proper error handling."""
    try:
        conn = sqlite3.connect(DATABASE)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        logger.error(f"Database connection error: {e}")
        return None

def init_db():
    """Initialize the database with required tables."""
    try:
        conn = get_db_connection()
        if conn:
            with conn:
                conn.execute('''CREATE TABLE IF NOT EXISTS sensor_data
                             (id INTEGER PRIMARY KEY AUTOINCREMENT,
                              timestamp TEXT,
                              temperature REAL,
                              humidity REAL,
                              soil_moisture INTEGER)''')
            logger.info("Database initialized")
        else:
            logger.error("Failed to initialize database")
    except Exception as e:
        logger.error(f"Database initialization error: {e}")

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
            payload = msg.payload.decode()
            if payload.count(',') == 2:
                temp, hum, soil = payload.split(',')
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                conn = get_db_connection()
                if conn:
                    try:
                        with conn:
                            conn.execute(
                                "INSERT INTO sensor_data (timestamp, temperature, humidity, soil_moisture) VALUES (?, ?, ?, ?)",
                                (timestamp, float(temp), float(hum), int(soil))
                            )
                        logger.info(f"Saved sensor data: {temp}°C, {hum}%, {soil}%")
                    finally:
                        conn.close()
            else:
                logger.warning(f"Invalid MQTT message format: {payload}")
                
    except Exception as e:
        logger.error(f"Error processing MQTT message: {e}")

def mqtt_thread():
    """Run MQTT client in a background thread."""
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    
    while True:
        try:
            logger.info("Attempting to connect to MQTT broker...")
            mqtt_client.connect(MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE)
            mqtt_client.loop_forever()
        except Exception as e:
            logger.error(f"MQTT connection error: {e}. Retrying in 10 seconds...")
            time.sleep(10)

def proses_data():
    """Process the latest sensor data and determine watering needs."""
    try:
        conn = get_db_connection()
        if not conn:
            return None, None, None, {"error": "Database connection failed"}
            
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT temperature, humidity, soil_moisture FROM sensor_data ORDER BY timestamp DESC LIMIT 1"
            )
            row = cursor.fetchone()
        finally:
            conn.close()
        
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

def signal_handler(sig, frame):
    """Handle shutdown signals gracefully."""
    logger.info("Received shutdown signal")
    try:
        mqtt_client.disconnect()
    except:
        pass
    try:
        scheduler.shutdown()
    except:
        pass
    sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Flask Routes
@app.route('/')
def dashboard():
    try:
        conn = get_db_connection()
        if not conn:
            return render_template('error.html', error="Database connection failed")
            
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM sensor_data ORDER BY timestamp DESC LIMIT 10")
            data = cursor.fetchall()
        finally:
            conn.close()
            
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

def create_app():
    """Application factory function."""
    # Initialize components
    init_db()
    
    # Start MQTT in background
    mqtt_thread_instance = threading.Thread(target=mqtt_thread)
    mqtt_thread_instance.daemon = True
    mqtt_thread_instance.start()
    
    # Start scheduler
    if not scheduler.running:
        scheduler.start()
        
        # Set timezone
        jakarta_tz = timezone('Asia/Jakarta')
        
        # Schedule proses_data at 7 AM and 5 PM
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
    
    return app

if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port=6000, debug=False)