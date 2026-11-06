
import socket
import json
import struct
import time


# =========================
# CONFIGURATION
# =========================

WORKER_ID = "002"
HOST = "127.0.0.1"
PORT = 5000

TEST_DELAY = 1


# =========================
# MESSAGE CREATION
# =========================

def create_worker_identifier():
    return {
        "type": "IDENTIFIER",
        "worker_id": WORKER_ID,
        "state": "READY",
    }


def create_task_result(task_id, process, result):
    return {
        "type": "TASK_RESULT",
        "worker_id": WORKER_ID,
        "task_id": task_id,
        "process": process,
        "message": result,
    }


# =========================
# NETWORK COMMUNICATION
# =========================

def send_msg(sock, data):
    """Send a message with a four-byte length prefix."""
    header = struct.pack(">I", len(data))
    sock.sendall(header + data)


def _recvn(sock, n):
    """Read exactly n bytes or raise if the connection closes."""
    buffer = bytearray()

    while len(buffer) < n:
        chunk = sock.recv(n - len(buffer))

        if not chunk:
            raise ConnectionError(
                "Connection closed before message was complete"
            )

        buffer.extend(chunk)

    return bytes(buffer)


def recv_msg(sock):
    """Receive one length-prefixed message."""
    raw_header = _recvn(sock, 4)
    message_length = struct.unpack(">I", raw_header)[0]

    return _recvn(sock, message_length)


def send_json(sock, message):
    """Encode a dictionary as JSON and send it."""
    data = json.dumps(message).encode("utf-8")
    send_msg(sock, data)


def receive_json(sock):
    """Receive and decode one JSON message."""
    data = recv_msg(sock)
    return json.loads(data.decode("utf-8"))


# =========================
# TASK PROCESSING
# =========================

def process_task(task):
    """Validate and process an assigned sensor analytics task."""
    worker_id = task.get("worker_id")
    task_id = task.get("task_id")
    process = task.get("process")
    readings = task.get("message")

    if worker_id != WORKER_ID:
        raise ValueError("Task assigned to the wrong worker")

    if not isinstance(task_id, (str, int)):
        raise ValueError("Invalid task ID")

    if not isinstance(readings, list) or not readings:
        raise ValueError("Expected a non-empty list of readings")

    for reading in readings:
        if not isinstance(reading, dict):
            raise ValueError("Each reading must be a dictionary")

        if not isinstance(reading.get("temperature"), (int, float)):
            raise ValueError("Invalid or missing temperature")

        if not isinstance(reading.get("humidity"), (int, float)):
            raise ValueError("Invalid or missing humidity")

    if process == "temperature_stats":
        temperatures = [
            reading["temperature"] for reading in readings
        ]

        result = {
            "count": len(temperatures),
            "minimum": min(temperatures),
            "maximum": max(temperatures),
            "average": round(sum(temperatures) / len(temperatures), 2),
        }

    elif process == "detect_anomalies":
        result = {
            "temperature_anomalies": [
                reading for reading in readings
                if reading["temperature"] >= 40
            ],
            "humidity_anomalies": [
                reading for reading in readings
                if reading["humidity"] >= 90
            ],
        }

    else:
        raise ValueError(f"Unsupported process: {process}")

    return create_task_result(task_id, process, result)


# =========================
# TASK HANDLING
# =========================

def handle_task(sock, task):
    """Process an assigned task and return its result."""
    if task.get("type") != "TASK":
        raise ValueError(
            f"Unexpected message type: {task.get('type')}"
        )

    print("Worker received:", task)

    # Temporary delay for testing task assignment and concurrency.
    time.sleep(TEST_DELAY)

    result = process_task(task)
    send_json(sock, result)

    print(f"Sent result for task {result['task_id']} to coordinator")


# =========================
# WORKER LIFECYCLE
# =========================

def run_worker():
    worker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        worker.connect((HOST, PORT))
        print(f"Connected to coordinator at {HOST}:{PORT}")

        send_json(worker, create_worker_identifier())
        print(f"Registered as worker {WORKER_ID}")

        while True:
            task = receive_json(worker)
            handle_task(worker, task)

    except ConnectionError as error:
        print(f"Connection closed: {error}")

    except (json.JSONDecodeError, ValueError, KeyError, TypeError) as error:
        print(f"Message or task error: {error}")

    except OSError as error:
        print(f"Socket error: {error}")

    finally:
        worker.close()
        print(f"Worker {WORKER_ID} shut down")


if __name__ == "__main__":
    run_worker()