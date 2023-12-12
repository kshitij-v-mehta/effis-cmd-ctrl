from multiprocessing.connection import Listener
from threading import Thread
import socket
import effis.signals as signals
from utils.logger import logger

_port = 6000

def server_thread(port, app_name, q):
    # Write connection info for the client to connect
    address = f"{socket.gethostname()}:{port}"
    logger.info(f"{app_name} writing connection info {address} to {app_name}.conn_info")
    with open(f"{app_name}.conn_info", "w") as f:
        f.write(f"{address}")

    # Start listening for connections
    logger.info(f"{app_name} listening for incoming connection")
    addr = (socket.gethostname(), port)
    listener = Listener(addr)
    conn = listener.accept()
    logger.info(f"{app_name} established connection.")

    # Recv ack from client that it is ready
    logger.info(f"{app_name} waiting for ready signal from client thread.")
    msg = conn.recv()
    logger.info(f"{app_name} received {msg} from client thread.")
    assert msg == signals.CLIENT_READY, f"EFFIS server thread for {app_name} received unknown acknowledgement {msg} instead of {signals.CLIENT_READY}"

    # Wait for a message
    msg = None
    if 'analysis' in app_name:
        # receive message from analysis client and forward it to the application
        logger.info(f"{app_name} waiting for message from client thread.")
        msg = conn.recv()
        
        logger.info(f"{app_name} received {msg} from client thread. Forwarding to effis server")
        q.put(msg)
    else:
        # extract signal from the queue from the analysis server and forward it to the simulation client
        logger.info(f"{app_name} waiting for message from effis server.")
        msg = q.get()
        q.task_done()
        
        logger.info(f"{app_name} received {msg} from effis server. Forwarding to app client thread.")
        conn.send(msg)

    # Close after you received a message
    logger.info(f"{app_name} closing connection")
    conn.close()


def _get_port():
    # port = 6000
    # while True:
    #     port += 1
    #     yield port

    global _port
    _port += 1
    return _port


def launch_server_thread(app_name, q):
    port = _get_port()
    logger.info(f"{app_name} launching server thread on port {port}")
    t = Thread(target=server_thread, args=(port, app_name, q))
    t.start()

    return t

