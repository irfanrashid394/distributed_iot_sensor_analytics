
import socket
import json
import struct
import selectors
import time
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

HOST = "127.0.0.1"
PORT = 5000
TASK_TIMEOUT = 30

selector = selectors.DefaultSelector()
STATUS_HOST = "127.0.0.1"
STATUS_PORT = 8000


class StatusHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/status":
            self.send_error(404)
            return

        worker_status = []

        for worker in list(workers.values()):
            worker_status.append({
                "worker_id": worker["worker_id"],
                "state": worker["state"],
                "current_task": worker["current_task"],
                "completed_tasks": worker_completed_counts.get(
                    worker["worker_id"], 0
                )
            })

        response = json.dumps({
            "workers": worker_status
        }).encode("utf-8")

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(response)

    def log_message(self, format, *args):
        pass


def start_status_server():
    server = HTTPServer((STATUS_HOST, STATUS_PORT), StatusHandler)
    print(f"Worker status endpoint: http://{STATUS_HOST}:{STATUS_PORT}/status")
    server.serve_forever()

# Connection-specific state
messages_per_connection = {}
workers = {}
outgoing_buffers = {}
task_start_times = {}
# Performance metrics
completed_task_count = 0
task_durations = []
worker_completed_counts = {}

benchmark_started_at = None
last_benchmark_activity_at = None
simulation_finished = False
benchmark_reported = False
last_task_completed_at = None

with open("task_record.json", "r") as file:
    task_record = json.load(file)


def recover_tasks_on_startup():
    recovered = 0

    for task_id, task in task_record.items():
        if task["status"] == "IN_PROGRESS":
            task["status"] = "PENDING"
            task["worker_id"] = None
            recovered += 1

    if recovered:
        with open("task_record.json", "w") as file:
            json.dump(task_record, file, indent=4)

    print(f"Startup recovery: {recovered} tasks recovered")


def update_worker_state(sock, state):
    if sock in workers:
        workers[sock]["state"] = state


def create_sensor_tasks(batch_id, readings):
    batch_id = str(batch_id)

    task_definitions = {
        f"sensor_{batch_id}_stats": {
            "process": "temperature_stats",
            "message": readings,
        },
        f"sensor_{batch_id}_anomalies": {
            "process": "detect_anomalies",
            "message": readings,
        },
    }

    created = 0

    for task_id, task_data in task_definitions.items():
        if task_id in task_record:
            continue

        task_record[task_id] = {
            "process": task_data["process"],
            "message": task_data["message"],
            "status": "PENDING",
            "worker_id": None,
            "result": None,
        }
        created += 1

    if created:
        with open("task_record.json", "w") as file:
            json.dump(task_record, file, indent=4)

    print(f"Created {created} analytics tasks for batch {batch_id}")


def assign_task(sock):
    worker = workers[sock]

    if worker["state"] != "READY":
        return

    for task_id, task in task_record.items():
        if task["status"] != "PENDING":
            continue

        task["worker_id"] = worker["worker_id"]
        task["status"] = "IN_PROGRESS"

        worker["state"] = "BUSY"
        worker["current_task"] = task_id
        task_start_times[task_id] = time.monotonic()

        with open("task_record.json", "w") as file:
            json.dump(task_record, file, indent=4)

        queue_task(sock, task_id, task)

        print(f"Assigned task {task_id} to worker {worker['worker_id']}")
        return


def queue_task(sock, task_id, task):
    payload = json.dumps({
        "type": "TASK",
        "worker_id": workers[sock]["worker_id"],
        "task_id": task_id,
        "process": task["process"],
        "message": task["message"],
    }).encode("utf-8")

    header = struct.pack(">I", len(payload))
    outgoing_buffers[sock].extend(header + payload)

    selector.modify(
        sock,
        selectors.EVENT_READ | selectors.EVENT_WRITE,
        data=workers[sock]["worker_id"]
    )


def send_pending_data(sock):
    buffer = outgoing_buffers[sock]

    try:
        sent = sock.send(buffer)
    except BlockingIOError:
        return

    del buffer[:sent]

    if not buffer:
        selector.modify(
            sock,
            selectors.EVENT_READ,
            data=workers[sock]["worker_id"]
        )

def handle_task_result(sock, result):
    global completed_task_count
    global last_benchmark_activity_at
    global last_task_completed_at

    worker = workers.get(sock)

    if worker is None:
        return

    worker_id = worker["worker_id"]
    task_id = str(result["task_id"])

    if str(result["worker_id"]) != worker_id:
        print("Error: result received from the wrong worker")
        return

    if worker["current_task"] != task_id:
        print("Error: result does not match worker's current task")
        return

    task = task_record.get(task_id)

    if task is None or task["status"] != "IN_PROGRESS":
        print("Error: task is missing or not in progress")
        return

    task["status"] = "COMPLETED"
    task["result"] = result["message"]
    last_task_completed_at = time.monotonic()
    last_benchmark_activity_at = last_task_completed_at

    started_at = task_start_times.pop(task_id, None)

    if started_at is not None:
        duration = time.monotonic() - started_at

        task["execution_seconds"] = round(duration, 3)
        task_durations.append(duration)

        
        completed_task_count += 1

        worker_completed_counts[worker_id] = (
            worker_completed_counts.get(worker_id, 0) + 1
        )

        print(f"Task {task_id} duration: {duration:.2f} seconds")

    with open("task_record.json", "w") as file:
        json.dump(task_record, file, indent=4)

    worker["state"] = "READY"
    worker["current_task"] = None

    print(f"Task {task_id} completed by worker {worker_id}")


def accept_connection(listener):
    conn, address = listener.accept()
    print("Accepted connection from:", address)

    conn.setblocking(False)

    messages_per_connection[conn] = bytearray()

    outgoing_buffers[conn] = bytearray()

    # The worker ID is unknown at this point.
    selector.register(conn, selectors.EVENT_READ, data=None)


def receive_messages(conn):
    try:
        chunk = conn.recv(4096)
    except BlockingIOError:
        return []

    if not chunk:
        return None

    buffer = messages_per_connection[conn]
    buffer.extend(chunk)

    messages = []

    while len(buffer) >= 4:
        message_length = struct.unpack(">I", buffer[:4])[0]

        if len(buffer) < 4 + message_length:
            break

        payload = bytes(buffer[4:4 + message_length])
        del buffer[:4 + message_length]

        messages.append(json.loads(payload.decode("utf-8")))

    return messages

def check_task_timeouts():
    now = time.monotonic()

    for task_id, start_time in list(task_start_times.items()):
        if now - start_time < TASK_TIMEOUT:
            continue

        task = task_record.get(task_id)

        if not task or task["status"] != "IN_PROGRESS":
            task_start_times.pop(task_id, None)
            continue

        worker_id = task.get("worker_id")

        worker_sock = next(
            (
                sock for sock, worker in workers.items()
                if worker["worker_id"] == worker_id
            ),
            None
        )

        task["status"] = "PENDING"
        task["worker_id"] = None
        task_start_times.pop(task_id, None)

        if worker_sock is not None:
            print(
                f"Worker {worker_id} timed out; closing connection "
                "to prevent conflicting task assignments"
            )

            selector.unregister(worker_sock)
            worker_sock.close()

            messages_per_connection.pop(worker_sock, None)
            outgoing_buffers.pop(worker_sock, None)
            workers.pop(worker_sock, None)

        with open("task_record.json", "w") as file:
            json.dump(task_record, file, indent=4)

        print(f"Task {task_id} timed out and returned to PENDING")



def maybe_print_final_benchmark():
    global benchmark_reported

    if benchmark_reported or not simulation_finished:
        return

    if benchmark_started_at is None:
        return

    # Do not report while any task is unfinished.
    if not task_record:
        return

    if any(
        task["status"] != "COMPLETED"
        for task in task_record.values()
    ):
        return

    # Wait until there has been 5 seconds without activity.
    if last_benchmark_activity_at is None:
        return

    if time.monotonic() - last_benchmark_activity_at < 5:
        return
    if last_task_completed_at is None:
        return

    elapsed = last_task_completed_at - benchmark_started_at

    print("\n========== FINAL BENCHMARK ==========")
    print(f"Tasks completed: {completed_task_count}")
    print(f"Benchmark duration: {elapsed:.3f} seconds")

    if elapsed > 0:
        print(
            f"Throughput: "
            f"{completed_task_count / elapsed:.3f} tasks/second"
        )
    else:
        print("Throughput: N/A")

    if task_durations:
        print(
            f"Average task duration: "
            f"{sum(task_durations) / len(task_durations):.4f} seconds"
        )
    else:
        print("Average task duration: N/A")

    print(f"Worker completions: {worker_completed_counts}")
    print("=====================================\n")
    average_duration = (
        sum(task_durations) / len(task_durations)
        if task_durations else None
    )

    benchmark_summary = {
        "tasks_completed": completed_task_count,
        "benchmark_duration_seconds": round(elapsed, 3),
        "throughput_tasks_per_second": (
            round(completed_task_count / elapsed, 3) if elapsed > 0 else None
        ),
        "average_task_duration_seconds": (
            round(average_duration, 4)
            if average_duration is not None else None
        ),
        "worker_completions": worker_completed_counts,
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }

    with open("benchmark_summary.json", "w", encoding="utf-8") as file:
        json.dump(benchmark_summary, file, indent=4)
        
    benchmark_reported = True


def main():
    global last_metrics_report
    global benchmark_started_at
    global simulation_finished
    global last_benchmark_activity_at

    recover_tasks_on_startup()
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((HOST, PORT))
    listener.listen()
    listener.setblocking(False)

    selector.register(listener, selectors.EVENT_READ)

    print(f"Coordinator listening on {HOST}:{PORT}")

    status_thread = threading.Thread(
        target=start_status_server,
        daemon=True
    )
    status_thread.start()
    
    try:
        while True:
            for key, mask in selector.select(timeout=1):
                sock = key.fileobj

                if sock is listener:
                    accept_connection(listener)
                    continue
                if mask & selectors.EVENT_WRITE:
                    send_pending_data(sock)

                    if sock.fileno() == -1:
                        continue
                messages = receive_messages(sock)

                
                if messages is None:
                    worker = workers.pop(sock, None)

                    if worker:
                        worker_id = worker["worker_id"]
                        task_id = worker["current_task"]

                        print("Disconnected:", worker_id)

                        if task_id is not None:
                            task_id = str(task_id)
                            task = task_record.get(task_id)

                            if task and task["status"] == "IN_PROGRESS":
                                task["status"] = "PENDING"
                                task["worker_id"] = None
                                task_start_times.pop(task_id, None)

                                with open("task_record.json", "w") as file:
                                    json.dump(task_record, file, indent=4)

                                print(f"Task {task_id} returned to PENDING")

                    selector.unregister(sock)
                    sock.close()
                    messages_per_connection.pop(sock, None)
                    outgoing_buffers.pop(sock, None)
                    continue

                for message in messages:

                    if message.get("type") == "IDENTIFIER":
                        worker_id = message.get("worker_id")

                        if not worker_id:
                            print("Invalid worker identifier")
                            continue

                        if any(
                            worker["worker_id"] == worker_id
                            for worker in workers.values()
                        ):
                            print("Duplicate worker ID:", worker_id)

                            selector.unregister(sock)
                            messages_per_connection.pop(sock, None)
                            outgoing_buffers.pop(sock, None)
                            sock.close()
                            break

                        workers[sock] = {
                            "worker_id": worker_id,
                            "state": message.get("state", "READY"),
                            "current_task": None,
                        }

                        selector.modify(
                            sock,
                            selectors.EVENT_READ,
                            data=worker_id
                        )

                        print("Registered worker:", worker_id)
                        assign_task(sock)
                        
                    elif message.get("type") == "TASK_RESULT":
                        handle_task_result(sock, message)
                        if sock in workers and workers[sock]["state"] == "READY":
                            assign_task(sock)
                        maybe_print_final_benchmark()

                    elif message.get("type") == "SENSOR_BATCH":
                        batch_id = message.get("batch_id")
                        readings = message.get("readings")

                        if batch_id is None:
                            print("Invalid sensor batch: missing batch_id")
                            continue

                        if not isinstance(readings, list) or not readings:
                            print("Invalid sensor batch: readings must be a non-empty list")
                            continue

                        
                        valid_readings = all(
                            isinstance(reading, dict)
                            and isinstance(reading.get("temperature"), (int, float))
                            and isinstance(reading.get("humidity"), (int, float))
                            and isinstance(reading.get("sensor_id"), str)
                            for reading in readings
                        )

                        if not valid_readings:
                            print("Invalid sensor batch: malformed readings")
                            continue

                        last_benchmark_activity_at = time.monotonic()
                        if benchmark_started_at is None:
                            benchmark_started_at = time.monotonic()
                            
                        create_sensor_tasks(batch_id, readings)

                        for worker_sock in list(workers):
                            if workers[worker_sock]["state"] == "READY":
                                assign_task(worker_sock)

                    elif message.get("type") == "SIMULATION_DONE":
                        simulation_finished = True
                        last_benchmark_activity_at = time.monotonic()
                        print("Sensor simulation finished submitting batches")
                        maybe_print_final_benchmark()

                    else:
                        print("Received:", message)

            check_task_timeouts()
            maybe_print_final_benchmark()

    finally:
        selector.close()
        listener.close()


if __name__ == "__main__":
    main()
