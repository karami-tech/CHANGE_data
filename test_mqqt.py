# print_and_save_mqtt_msgpack.py
import json
from datetime import datetime
import msgpack
import paho.mqtt.client as mqtt

MQTT_BROKER = "145.38.192.185"   # your server IP
MQTT_PORT   = 1883
MQTT_TOPIC  = "trailer/sensor-data"  # match Node-RED topic
OUTPUT_FILE = "received_data2.txt"

def save_to_file(data, topic):
    """Append a decoded message to a text file."""
    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        f.write(f"\n\n=== {datetime.utcnow().isoformat()}Z | topic={topic} ===\n")
        json.dump(data, f, indent=2, ensure_ascii=False)

def on_connect(client, userdata, flags, reason_code, properties=None):
    print(f"✅ Connected (reason_code={reason_code}). Subscribing to {MQTT_TOPIC} ...")
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    try:
        decoded = msgpack.unpackb(msg.payload, raw=False)
        print(f"\n📩 {datetime.utcnow().isoformat()}Z  topic={msg.topic}")
        print(json.dumps(decoded, indent=2, ensure_ascii=False))
        save_to_file(decoded, msg.topic)
    except Exception as e:
        print(f"❌ Failed to decode MsgPack from {msg.topic}: {e}")

def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT)
    print(f"📡 Listening at {MQTT_BROKER}:{MQTT_PORT} on topic '{MQTT_TOPIC}' ... Ctrl+C to stop.")
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n👋 Bye")

if __name__ == "__main__":
    main()
