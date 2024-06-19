import socket
import threading
import os
import sys
import signal
import logging
import time

request_count = 0
failure_threshold = 5
pid_file = "/run/multithreaded_server.pid"
max_retries = 5

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

def handle_client_connection(client_socket):
    global request_count
    try:
        request = client_socket.recv(1024).decode()
        logging.info(f"Received request:\n{request}")

        request_count += 1
        if request_count >= failure_threshold:
            logging.error("Simulating failure!")
            os._exit(1)  # Simulate a crash

        http_response = """\
HTTP/1.1 200 OK

Hello, World!
"""
        client_socket.sendall(http_response.encode())
    finally:
        client_socket.close()

def start_server():
    retries = 0
    while retries < max_retries:
        try:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(('0.0.0.0', 8000))
            server.listen(5)
            logging.info("Server started on port 8000")
            break  # Exit the loop if binding is successful
        except OSError as e:
            logging.error(f"Failed to bind to port 8000: {e}")
            retries += 1
            if retries >= max_retries:
                logging.error("Max retries reached. Exiting.")
                sys.exit(1)
            logging.info(f"Retrying in 5 seconds... ({retries}/{max_retries})")
            time.sleep(5)

    def handle_exit(signum, frame):
        logging.info("Shutting down server")
        server.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_exit)
    signal.signal(signal.SIGINT, handle_exit)

    while True:
        try:
            client_sock, addr = server.accept()
            logging.info(f"Accepted connection from {addr}")
            client_handler = threading.Thread(target=handle_client_connection, args=(client_sock,))
            client_handler.start()
        except Exception as e:
            logging.error(f"Error accepting connection: {e}")

def write_pid_file():
    try:
        with open(pid_file, "w") as f:
            f.write(str(os.getpid()))
    except IOError as e:
        logging.error(f"Failed to write PID file {pid_file}: {e}")
        sys.exit(1)

def remove_pid_file():
    try:
        if os.path.exists(pid_file):
            os.remove(pid_file)
    except OSError as e:
        logging.error(f"Failed to remove PID file {pid_file}: {e}")

def signal_handler(signum, frame):
    logging.info(f"Received signal {signum}. Exiting...")
    remove_pid_file()
    sys.exit(0)

def daemonize():
    if os.path.exists(pid_file):
        logging.error(f"PID file {pid_file} already exists. Is the server already running?")
        sys.exit(1)

    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError as e:
        logging.error(f"Fork #1 failed: {e}")
        sys.exit(1)

    os.setsid()

    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError as e:
        logging.error(f"Fork #2 failed: {e}")
        sys.exit(1)

    # Close file descriptors
    sys.stdout.flush()
    sys.stderr.flush()
    with open('/dev/null', 'r') as dev_null:
        os.dup2(dev_null.fileno(), sys.stdin.fileno())
    with open('/dev/null', 'a+') as dev_null:
        os.dup2(dev_null.fileno(), sys.stdout.fileno())
        os.dup2(dev_null.fileno(), sys.stderr.fileno())

    write_pid_file()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    start_server()

if __name__ == "__main__":
    try:
        daemonize()
    except Exception as e:
        logging.error(f"Error in daemonizing: {e}")
        raise

