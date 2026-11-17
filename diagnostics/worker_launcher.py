import os
import sys
import traceback
from datetime import datetime
from pathlib import Path

LOG_FILE = Path(__file__).resolve().parent / "worker_error.log"

def log(message):
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(f"{datetime.now()} | {message}\n")

log("=== PYTHON WORKER START ===")
log(f"Executable: {sys.executable}")
log(f"PID: {os.getpid()}")
log(f"Arguments: {sys.argv}")
log(f"PYTHONPATH: {os.environ.get('PYTHONPATH')}")
log(f"SPARK_LOCAL_IP: {os.environ.get('SPARK_LOCAL_IP')}")

try:
    import pyspark.worker

    log("pyspark.worker imported successfully")

    from pyspark.worker import main

    log("Starting worker main")

    main(sys.stdin.buffer, sys.stdout.buffer)

    log("Worker finished")

except BaseException:
    with LOG_FILE.open("a", encoding="utf-8") as f:
        traceback.print_exc(file=f)
    raise