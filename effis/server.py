from multiprocessing.connection import Listener
from threading import Thread
import os, socket
import effis.signals as signals
from utils.logger import logger

_port = 6000

def server_thread(port, app_name, q, thread_type):
    """Start an effis server thread.
    port is where the socket will start
    app_name is for logging purposes only
    q is a shared message queue for communicating with other server threads via the effis server
    thread_type = listener/sender. A listener thread listens to incoming messages,
    whereas a sender thread will send signals to effis. Server threads for the main 
    simulation must be of type listener whereas those for analysis apps must be senders.
    """
    # Write connection info for the client to connect
    address = f"{socket.gethostname()}:{port}"
    logger.debug(f"{app_name} writing connection info {address} to {app_name}.conn_info")
    conn_info = f"{app_name}.conn_info" 
    with open(conn_info, "w") as f:
        f.write(f"{address}")

    # Start listening for connections
    logger.debug(f"{app_name} listening for incoming connection")
    addr = (socket.gethostname(), port)
    listener = Listener(addr)
    conn = listener.accept()
    logger.info(f"{app_name} established connection.")

    # Recv ack from client that it is ready
    logger.debug(f"{app_name} waiting for ready signal from client thread.")
    msg = conn.recv()
    logger.debug(f"{app_name} received {msg} from client thread.")
    assert msg == signals.CLIENT_READY, f"EFFIS server thread for {app_name} received unknown acknowledgement {msg} instead of {signals.CLIENT_READY}"

    # Wait for a message
    msg = ""
    while all(signal not in msg for signal in ["TERM", "DONE"]):
        if thread_type == 'sender':
            # receive message from analysis client and forward it to the application
            logger.debug(f"{app_name} waiting for message from client thread.")
            msg = conn.recv()
            
            logger.debug(f"{app_name} received {msg} from client thread. Forwarding to effis server")
            q.put(msg)
        elif thread_type == 'listener':
            # extract signal from the queue from the analysis server and forward it to the simulation client
            logger.debug(f"{app_name} waiting for message from effis server.")
            msg = q.get()
            q.task_done()
            
            logger.debug(f"{app_name} received {msg} from effis server. Forwarding to app client thread.")
            conn.send(msg)
        else:
            raise Exception(f"Cannot understand type {thread_type} of server thread to launch for {app_name}")

    # Close after you received a message
    logger.info(f"{app_name} closing connection")
    conn.close()
    try:
        os.remove(conn_info)
    except FileNotFoundError:
        pass


def _get_port():
    # port = 6000
    # while True:
    #     port += 1
    #     yield port

    global _port
    _port += 1
    return _port


def launch_server_thread(app_name, q, thread_type):
    port = _get_port()
    logger.debug(f"{app_name} launching server thread on port {port}")
    t = Thread(target=server_thread, args=(port, app_name, q, thread_type))
    t.start()

    return t

