from multiprocessing.connection import Client
from threading import Thread
import effis.signals as effis_signals
from utils.logger import logger


def _heartbeat_monitor(app_name, hbq, server_addr):
    """
    Target function for the heartbeat thread.
    This thread is a deamon thread and will exit when the main app exits.

    Args:
    app_name (string)   : Name of the application. For logging purposes
    hbq (queue)         : Queue for receiving heartbeat message from the app's root rank
    server_addr (tuple) : (hostname, port) address of the heartbeat server thread
    """
    # Establish connection with effis server
    conn = Client(server_addr)
    logger.info(f"{app_name} heartbeat thread connection established")

    # Monitor heartbeat queue for heartbeat message from the simulation and send it to effis
    while True:
        hb_msg = hbq.get()
        logger.info(f"Client thread received heartbeat from simulation. Forwarding to effis.")
        conn.send(hb_msg)

def _listener(app_name, q, conn):
    """
    I am a listener thread. I listen for incoming signals from the science app (analysis codes) and forward them to effis
    """
    signal = ""
    while all(s not in signal for s in ["TERM", "DONE"]):
        # Get signal from the application
        logger.debug(f"{app_name} waiting for signal in queue.")
        signal = q.get()
        q.task_done()

        # forward the signal to the effis server
        logger.debug(f"{app_name} received {signal}. Forwarding to effis.")
        conn.send(signal)

    logger.debug(f"{app_name} client thread received {signal}. Returning.")

def _sender(app_name, q, hbq, conn, hbt_server_addr):
    """
    I am a sender thread. I send signals from the effis server thread to the science app (simulation).
    """
    # Launch thread for sending heartbeat periodically
    if hbq is not None:
        logger.info(f"Launching heartbeat client thread for {app_name}")
        hbt = Thread(target=_heartbeat_monitor, args=(app_name, hbq, hbt_server_addr))
        hbt.daemon = True
        hbt.start()

    signal = ""
    while all(s not in signal for s in ["TERM", "DONE"]):
        # Receive signal
        logger.debug(f"{app_name} waiting for signal from effis server.")
        signal = conn.recv()

        # Forward it to the application
        logger.debug(f"{app_name} received {signal} from effis server. Forwarding to application via shared queue.")
        q.put(signal)

    # Tell heartbeat monitor to quit.
    # hbq.put("QUIT")
    # hbt.join()
    logger.debug(f"{app_name} received {signal}. Returning.")

def effis_client(app_name, q, hbq, thread_type):
    # Start connection with the effis server
    logger.debug(f"{app_name} Looking for connection info in {app_name}.conn_info")
    with open(f"{app_name}.conn_info") as f:
        conn_info = f.readline()
        hostname = conn_info.split(":")[0]
        port = int(conn_info.split(":")[1])
    
    address = (hostname, port)
    logger.debug(f"{app_name} found connection info {address}")

    conn = Client(address)
    logger.info(f"{app_name} connection established")

    # Send an ack that I am ready
    logger.debug(f"{app_name} sending ready acknowledgement")
    conn.send(effis_signals.CLIENT_READY)

    # The heartbeat server thread will listen on the following addr
    hbt_server_addr = (hostname, port+1)

    if thread_type == 'listener':
        _listener(app_name, q, conn)
    else:
        _sender(app_name, q, hbq, conn, hbt_server_addr)
    
    logger.info(f"{app_name} exiting effis_client.")

