import socket
import subprocess
import sys

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(("127.0.0.1", 0))
server.listen(1)
server.settimeout(10)

port = server.getsockname()[1]

print("Python:", sys.executable)
print("Port:", port)

worker_code = """
import socket
import sys

port = int(sys.argv[1])

with socket.create_connection(("127.0.0.1", port), timeout=5) as s:
    s.sendall(b"PYTHON_WORKER_OK")
"""

process = subprocess.Popen([sys.executable, "-c", worker_code, str(port)])

try:
    conn, addr = server.accept()

    with conn:
        data = conn.recv(1024)

    print("Received:", data.decode())
    print("Worker exit code:", process.wait(timeout=5))

finally:
    server.close()

    if process.poll() is None:
        process.kill()
        process.wait()
