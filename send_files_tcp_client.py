"""
The idea of the client is that it has to be small and self-contained,
 in order to allow integration without problems with existing scripts
"""
import os
import socket
import time
import configparser
import pickle

FILENAME_OK = "FILE_OK"
FILENAME_NOK = "FILE_NOK"
SOCKET_SEND_OK = "SEND_OK"
SOCKET_ERROR_WHEN_SENDING_FILENAME = "SOCKET_ERROR_FILENAME"
SOCKET_ERROR_WHEN_SENDING_DATA = "SOCKET_ERROR_DATA"
SOCKET_ERROR_WHEN_SHUTTING_DOWN = "SOCKET_ERROR_SHUTTING_DOWN"

SERVER_CONFIG_FILE = "/etc/radiation-benchmarks.conf"
BUFFER_SIZE = 4096
OBJ_LENGTH_WIDTH = 4
CLIENT_SOCKET_TIMEOUT = 5

try:
    config = configparser.ConfigParser()
    config.read(SERVER_CONFIG_FILE)
    SERVER_ADDRESS = config["DEFAULT"]["server_address"]
    SERVER_PORT = config["DEFAULT"]["server_port"]
except (FileNotFoundError, configparser.Error, KeyError):
    SERVER_PORT = 5001  # must match server
    SERVER_ADDRESS = "127.0.0.1"


def send_bytes_to_server(obj: object, filename: str, server_address: str, server_port: int) -> str:
    """Send obj to the server.
    Protocol: send filename -> wait ack -> send payload length -> send payload.
    Returns: SOCKET_SEND_OK if ok, otherwise an error string.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(CLIENT_SOCKET_TIMEOUT)
        sock.connect((server_address, server_port))

        # send filename first (same as your code)
        sock.sendall(os.path.basename(filename).encode("utf-8"))

        # wait for ack
        ack = sock.recv(BUFFER_SIZE).decode("utf-8", errors="replace")
        if ack != FILENAME_OK:
            sock.close()
            return FILENAME_NOK
    except socket.error:
        return SOCKET_ERROR_WHEN_SENDING_FILENAME

    try:
        data_bytes = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)

        payload_len = len(data_bytes)
        sock.sendall(payload_len.to_bytes(OBJ_LENGTH_WIDTH, "big"))
        sock.sendall(data_bytes)
    except socket.error:
        try:
            sock.close()
        finally:
            return SOCKET_ERROR_WHEN_SENDING_DATA

    try:
        sock.shutdown(socket.SHUT_WR)
        sock.close()
    except socket.error:
        return SOCKET_ERROR_WHEN_SHUTTING_DOWN

    return SOCKET_SEND_OK


def debug():
    from multiprocessing import Process
    import torch

    def process_sender(server_address: str, server_port: int):
        dummy = torch.randn(5, 10)
        for i in range(1, 1000):
            print(dummy)
            returned = send_bytes_to_server(obj=dummy, filename="dummy_tensor.pt",
                                            server_address=server_address, server_port=server_port)
            print(returned)
            time.sleep(10)

    procs = []
    for server_port in [5001, 5003, 5004, 5005]:
        proc = Process(target=process_sender, args=(SERVER_ADDRESS, server_port))
        proc.start()
        procs.append(proc)

    try:
        # keep main thread alive
        while True:
            time.sleep(0.25)
    except KeyboardInterrupt:
        for p in procs:
            p.join()


if __name__ == "__main__":
    debug()
