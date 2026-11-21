import os
import subprocess
import sys

print("Python:", sys.executable)

print("\n=== PYSPARK ENVIRONMENT ===")

for key in [
    "PYSPARK_PYTHON",
    "PYSPARK_DRIVER_PYTHON",
    "PYTHONPATH",
    "SPARK_HOME",
]:
    print(f"{key}: {os.environ.get(key)}")

print("\n=== WORKER IMPORT ===")

result = subprocess.run(
    [
        sys.executable,
        "-c",
        (
            "import pyspark.worker; "
            "import pyspark.daemon; "
            "print('PYSPARK_WORKER_IMPORT_OK')"
        ),
    ],
    check=False,
    capture_output=True,
    text=True,
    timeout=15,
)

print("Return code:", result.returncode)
print("STDOUT:", result.stdout)
print("STDERR:", result.stderr)
