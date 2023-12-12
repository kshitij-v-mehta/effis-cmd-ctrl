from multiprocessing.connection import Client
import effis.signals as effis_signals
from utils.logger import logger


def effis_client(app_name, q):
    # Start connection with the effis server
    logger.info(f"{app_name} Looking for connection info in {app_name}.conn_info")
    with open(f"{app_name}.conn_info") as f:
        conn_info = f.readline()
        hostname = conn_info.split(":")[0]
        port = int(conn_info.split(":")[1])
    
    address = (hostname, port)
    logger.info(f"{app_name} found connection info {address}")

    conn = Client(address)
    logger.info(f"{app_name} connection established")

    # Send an ack that I am ready
    logger.info(f"{app_name} sending ready acknowledgement")
    conn.send(effis_signals.CLIENT_READY)

    # Tell the application that I am ready
    logger.info(f"{app_name} sending ready acknowledgement to application in shared queue")
    q.put(effis_signals.CLIENT_READY)

    # Now start monitoring for a signal
    if "analysis" in app_name:
        # Get signal from the application
        logger.info(f"{app_name} waiting for signal in queue.")
        signal = q.get()
        q.task_done()

        # forward the signal to the effis server
        logger.info(f"{app_name} received {signal}. Forwarding to effis.")
        conn.send(signal)

    else:
        # Receive signal
        logger.info(f"{app_name} waiting for signal from effis server.")
        signal = conn.recv()

        # Forward it to the application
        logger.info(f"{app_name} received {signal} from effis server. Forwarding to application via shared queue.")
        q.put(signal)

    logger.info(f"{app_name} exiting effis_client.")

