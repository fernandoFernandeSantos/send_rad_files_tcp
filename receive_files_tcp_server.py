# server
import pickle
import logging
import os
import socket
import threading
import time
from typing import Optional

from send_files_tcp_client import FILENAME_OK, OBJ_LENGTH_WIDTH, BUFFER_SIZE, CLIENT_SOCKET_TIMEOUT

SERVER_IP = "127.0.0.1"
# Not to be changed
SOCKET_BACKLOG = 5
DOWNLOADED_FILES_DIR = "files_downloaded"
SOCKET_ACCEPT_TIMEOUT = 5

# Test with different machines
CLIENT_MACHINES = [
    {"listen_port": 5000 + mid} for mid in [1, 3, 4, 5]
]


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


def _recv_exact(client_socket: socket.socket, length: int) -> bytes:
    """Receive exactly `length` bytes from `client_socket` or raise ConnectionError."""
    buf = bytearray()
    while len(buf) < length:
        try:
            chunk = client_socket.recv(length - len(buf))
        except socket.timeout as e:
            raise ConnectionError("Receive timeout while waiting for data") from e
        except OSError as e:
            raise ConnectionError(f"Socket error during recv: {e}") from e
        if not chunk:
            # connection closed or error
            raise ConnectionError("Socket closed before receiving all data")

        buf.extend(chunk)
    return bytes(buf)


def _recv_obj(client_socket: socket.socket) -> bytes:
    """Receive a length-prefixed pickled object as raw bytes."""
    # receive the size prefix
    len_bytes = _recv_exact(client_socket, OBJ_LENGTH_WIDTH)
    obj_length = int.from_bytes(len_bytes, 'big')

    # Then read exactly `obj_length` bytes
    obj_bytes = _recv_exact(client_socket, obj_length)
    return obj_bytes


class ReceiveFilesTCPServerThread(threading.Thread):
    def __init__(
            self,
            server_port: int,
            server_host: str,
            socket_backlog: int,
            logger: logging.Logger,
            thread_event: threading.Event,
    ):
        super().__init__(daemon=True)
        self.server_port = server_port
        self.server_host = server_host
        self.logger = logger
        self.thread_event = thread_event

        # Setup listening socket
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(SOCKET_ACCEPT_TIMEOUT)
        self.socket.bind((server_host, server_port))
        self.socket.listen(socket_backlog)

    def join(self, timeout: Optional[float] = None):
        """Signal thread to stop, close listening socket, then join."""
        self.logger.info(f"[*] Joining thread {self.server_host}:{self.server_port}")
        try:
            self.socket.close()
        except OSError:
            pass
        super().join(timeout)

    def run(self):
        self.logger.info(f"[*] Listening on {self.server_host}:{self.server_port}")

        while not self.thread_event.is_set():
            try:
                client_socket, addr = self.socket.accept()
                client_socket.settimeout(CLIENT_SOCKET_TIMEOUT)
                self.logger.info(f"[+] {addr} connected.")
            except OSError:
                # Socket likely closed during shutdown
                if self.thread_event.is_set():
                    break
                continue

            # Ensure client socket is always closed
            with client_socket:
                try:
                    # receive filename first (simple protocol: one recv)
                    try:
                        raw_name = client_socket.recv(BUFFER_SIZE)
                        filename = raw_name.decode(errors="replace").strip()
                        self.logger.info(f"Received filename from {addr}")
                    except (socket.timeout, OSError) as e:
                        self.logger.error(f"Error receiving filename from {addr}: {e}")
                        continue

                    # send ACK for filename
                    try:
                        client_socket.sendall(FILENAME_OK.encode())
                    except (socket.timeout, OSError) as e:
                        self.logger.error(f"Failed to send ACK to {addr}: {e}")
                        continue

                    # open file to write binary data
                    current_clock = int(time.time())
                    filename_with_ts = f"{current_clock}_{filename}"

                    addr_path = os.path.join(DOWNLOADED_FILES_DIR, addr[0])
                    os.makedirs(addr_path, exist_ok=True)
                    client_file_path = os.path.join(addr_path, filename_with_ts)

                    # Receive full object bytes
                    try:
                        obj_bytes = _recv_obj(client_socket)
                        # print_byte_array(obj_bytes)
                    except ConnectionError as e:
                        self.logger.error(f"Failed to receive object from {addr}: {e}")
                        continue

                    try:
                        with open(client_file_path, "wb") as file:
                            pickle.dump(obj_bytes, file)
                    except Exception as e:
                        self.logger.error(f"Failed to save from {addr}: {e}")
                        continue

                    self.logger.info(f"File '{filename_with_ts}' received and saved from {addr}.")

                except Exception as e:
                    # Catch any unexpected error handling this client
                    self.logger.exception(f"Unhandled error while handling client {addr}: {e}")
                    continue


# def print_byte_array(byte_array: bytes):
#     obj = pickle.loads(byte_array)
#     print(obj)


def main():
    logger = get_logger("main")
    machines_receive_threads = []

    if not os.path.isdir(DOWNLOADED_FILES_DIR):
        os.makedirs(DOWNLOADED_FILES_DIR)

    thread_event = threading.Event()

    try:
        # spawn threads
        for machine in CLIENT_MACHINES:
            server_port = machine["listen_port"]
            new_thread = ReceiveFilesTCPServerThread(
                server_port=server_port,
                server_host=SERVER_IP,
                socket_backlog=SOCKET_BACKLOG,
                logger=logger,
                thread_event=thread_event
            )
            machines_receive_threads.append(new_thread)
            new_thread.start()

        logger.info("All receiver threads started. Press CTRL-C to exit.")

        # keep main thread alive
        while not thread_event.is_set():
            time.sleep(0.25)

    except KeyboardInterrupt:
        logger.warning("CTRL-C pressed, shutting down...")
        thread_event.set()

    finally:
        # Always join threads during shutdown
        for t in machines_receive_threads:
            t.join()
        logger.info("All receiver threads stopped. Bye!")


if __name__ == "__main__":
    main()
