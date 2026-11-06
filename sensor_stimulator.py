
import json
import random
import socket
import struct
import time
from datetime import datetime, timezone

HOST = "127.0.0.1"
PORT = 5000
BATCH_SIZE = 20
BATCH_INTERVAL = 1
MAX_BATCHES = 10


def generate_readings(batch_id):
    readings = []

    for i in range(BATCH_SIZE):
        readings.append({
            "sensor_id": f"sensor_{i % 5 + 1}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "temperature": round(random.gauss(24, 2), 2),
            "humidity": round(random.gauss(55, 5), 2),
        })

    # Inject known anomalies periodically for evaluation.
    if batch_id % 3 == 0:
        readings[0]["temperature"] = 45.0
        readings[1]["humidity"] = 98.0

    return readings


def send_json(sock, data):
    payload = json.dumps(data).encode("utf-8")
    sock.sendall(struct.pack(">I", len(payload)) + payload)


def main():
    for batch_id in range(1, MAX_BATCHES + 1):
        readings = generate_readings(batch_id)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((HOST, PORT))

            send_json(sock, {
                "type": "SENSOR_BATCH",
                "batch_id": batch_id,
                "readings": readings,
            })

        print(f"Submitted sensor batch {batch_id}")

        if batch_id < MAX_BATCHES:
            time.sleep(BATCH_INTERVAL)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((HOST, PORT))
                send_json(sock, {
                    "type": "SIMULATION_DONE",
                    "batch_count": MAX_BATCHES,
                })
    print(f"Sensor simulation finished: {MAX_BATCHES} batches submitted")



if __name__ == "__main__":
    main()
