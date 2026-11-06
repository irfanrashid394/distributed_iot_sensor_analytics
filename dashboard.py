import json
from pathlib import Path
import pandas as pd
import urllib.request
import urllib.error
import streamlit as st
from streamlit_autorefresh import st_autorefresh

#JOIN USING http://localhost:8501/

PROJECT_DIR = Path(__file__).resolve().parent
TASK_RECORD_PATH = PROJECT_DIR / "task_record.json"
BENCHMARK_PATH = PROJECT_DIR / "benchmark_summary.json"

st.set_page_config(page_title="Distributed IoT Sensor Analytics", page_icon="📡", layout="wide")
st.title("Distributed IoT Sensor Analytics")
st.caption("Local, read-only dashboard · refreshes every 3 seconds")
st_autorefresh(interval=3000, key="dashboard_refresh")

def read_worker_status():
    url = "http://127.0.0.1:8000/status"

    try:
        with urllib.request.urlopen(url, timeout=2) as response:
            return json.loads(response.read().decode("utf-8")), None
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        return None, str(exc)


st.subheader("Live worker status")

worker_data, worker_error = read_worker_status()

if worker_error:
    st.warning(
        "Could not retrieve live worker status. "
        "Make sure the coordinator and its status server are running."
    )
else:
    workers = worker_data.get("workers", [])

    if not workers:
        st.info("No workers are currently connected.")

    worker_columns = st.columns(2)

    for index, worker_id in enumerate(["001", "002"]):
        worker = next(
            (
                item for item in workers
                if str(item.get("worker_id")) == worker_id
            ),
            None
        )

        with worker_columns[index]:
            st.markdown(f"### Worker {worker_id}")

            if worker is None:
                st.error("DISCONNECTED")
            else:
                state = worker.get("state", "UNKNOWN")

                if state == "READY":
                    st.success("READY")
                elif state == "BUSY":
                    st.warning("BUSY")
                else:
                    st.info(state)

                st.write(
                    "**Current task:**",
                    worker.get("current_task") or "None"
                )
                st.metric(
                    "Tasks completed",
                    worker.get("completed_tasks", 0)
                )

def read_json(path):
    if not path.exists():
        return None, f"{path.name} not found"
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f), None
    except (json.JSONDecodeError, OSError) as exc:
        return None, f"Could not read {path.name}: {exc}"


task_record, task_error = read_json(TASK_RECORD_PATH)
benchmark, benchmark_error = read_json(BENCHMARK_PATH)
if task_record is None:
    st.error(task_error)
    st.info("Put dashboard.py in the same project folder as task_record.json, then start the dashboard again.")
    st.stop()
if not isinstance(task_record, dict):
    st.error("task_record.json must contain a JSON object mapping task IDs to task records.")
    st.stop()

rows = []
for task_id, task in task_record.items():
    if isinstance(task, dict):
        rows.append({
            "Task ID": task_id,
            "Operation": task.get("process", "—"),
            "Status": task.get("status", "UNKNOWN"),
            "Worker": task.get("worker_id") or "—",
            "Duration (s)": task.get("execution_seconds"),
            "Result": task.get("result"),
        })
df = pd.DataFrame(rows, columns=["Task ID", "Operation", "Status", "Worker", "Duration (s)", "Result"])
total = len(df)
completed = int((df["Status"] == "COMPLETED").sum()) if total else 0
in_progress = int((df["Status"] == "IN_PROGRESS").sum()) if total else 0
pending = int((df["Status"] == "PENDING").sum()) if total else 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Tasks recorded", total)
c2.metric("Completed", completed)
c3.metric("In progress", in_progress)
c4.metric("Pending", pending)
if total:
    st.progress(completed / total, text=f"{completed} of {total} tasks completed ({completed / total:.0%})")
else:
    st.info("No tasks recorded yet. Start the sensor simulator to submit batches.")

left, right = st.columns([1.2, 1])
with left:
    st.subheader("Task progress")
    st.dataframe(df[["Task ID", "Operation", "Status", "Worker", "Duration (s)"]], use_container_width=True, hide_index=True)
with right:
    st.subheader("Completed tasks by worker")
    done = df[df["Status"] == "COMPLETED"] if total else df
    counts = done[done["Worker"] != "—"]["Worker"].value_counts().sort_index()
    if not counts.empty:
        st.bar_chart(counts.rename("Completed tasks"))
        st.dataframe(counts.rename_axis("Worker").reset_index(name="Completed tasks"), use_container_width=True, hide_index=True)
    else:
        st.info("Worker completion counts appear when tasks finish.")

st.subheader("Analytics results")
results = df[(df["Status"] == "COMPLETED") & df["Result"].notna()] if total else df
if not results.empty:
    task_id = st.selectbox("Choose a completed task", results["Task ID"].tolist())
    selected = results.loc[results["Task ID"] == task_id].iloc[0]
    st.write(f"**Operation:** {selected['Operation']} · **Worker:** {selected['Worker']}")
    st.json(selected["Result"])
else:
    st.info("Completed analytics results will appear here.")

st.subheader("Last performance benchmark")
if benchmark_error:
    st.info("No benchmark_summary.json yet. Follow COORDINATOR_PATCH.md to save the final benchmark from the coordinator.")
elif isinstance(benchmark, dict):
    b1, b2, b3 = st.columns(3)
    duration = benchmark.get("benchmark_duration_seconds")
    throughput = benchmark.get("throughput_tasks_per_second")
    average = benchmark.get("average_task_duration_seconds")
    b1.metric("Duration", f"{duration:.3f} s" if isinstance(duration, (int, float)) else "—")
    b2.metric("Throughput", f"{throughput:.3f} tasks/s" if isinstance(throughput, (int, float)) else "—")
    b3.metric("Average task duration", f"{average:.4f} s" if isinstance(average, (int, float)) else "—")
    st.caption(f"Tasks completed in last benchmark: {benchmark.get('tasks_completed', '—')}")
    if benchmark.get("worker_completions"):
        st.write("Worker completions in last benchmark")
        st.json(benchmark["worker_completions"])
    if benchmark.get("generated_at"):
        st.caption(f"Saved at: {benchmark['generated_at']}")
else:
    st.info("Benchmark summary is empty or has an unexpected format.")

st.divider()
st.caption("Read-only view. This dashboard does not start or stop workers, assign tasks, or modify task records. Worker counts come from persisted task records; live READY/BUSY socket state is not exposed yet.")
