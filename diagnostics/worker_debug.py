import os
import sys
import traceback
from datetime import datetime
from pathlib import Path

LOG_FILE = Path(__file__).resolve().parent / "worker_error.log"

with LOG_FILE.open("a", encoding="utf-8") as log:
    log.write(f"\n=== WORKER START {datetime.now()} ===\n")
    log.write(f"Python: {sys.executable}\n")
    log.write(f"PID: {os.getpid()}\n")

try:
    from pyspark.worker import main

    with LOG_FILE.open("a", encoding="utf-8") as log:
        log.write("Worker module imported successfully\n")

    main(sys.stdin.buffer, sys.stdout.buffer)

except BaseException:
    with LOG_FILE.open("a", encoding="utf-8") as log:
        traceback.print_exc(file=log)
    raise
