from multiprocessing.connection import Listener
from threading import Thread
import os, socket
import effis.signals as signals
from utils.logger import logger

_port = 6000


def server_thread(app_name, address, q, thread_type):
    """Start an effis server thread.
    port is where the socket will start
    app_name is for logging purposes only
    q is a shared message queue for communicating with other server threads via the effis server
    thread_type = listener/sender. A listener thread listens to incoming messages,
    whereas a sender thread will send signals to effis. Server threads for the main 
    simulation must be of type listener whereas those for analysis apps must be senders.
    """
    # Write connection info for the client to connect
    logger.debug(f"{app_name} writing connection info {address} to {app_name}.conn_info")
    conn_info = f"{app_name}.conn_info" 
    with open(conn_info, "w") as f:
        f.write(f"{address[0]}:{address[1]}")

    # Start listening for connections
    logger.debug(f"{app_name} listening for incoming connection")
    listener = Listener(address)
    conn = listener.accept()
    logger.info(f"{app_name} established connection.")

    # Recv ack from client that it is ready
    logger.debug(f"{app_name} waiting for ready signal from client thread.")
    msg = conn.recv()
    logger.debug(f"{app_name} received {msg} from client thread.")
    assert msg == signals.CLIENT_READY, f"EFFIS server thread for {app_name} received unknown acknowledgement {msg} instead of {signals.CLIENT_READY}"

    # Wait for a message
    msg = ""
    while all(signal not in msg for signal in ["TERM", "DONE", "QUIT"]):
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

    conn.close()
    try:
        os.remove(conn_info)
    except FileNotFoundError:
        pass
    finally:
        # Close after you received a message
        logger.info(f"{app_name} closing connection. Exiting effis server.")


def get_port():
    # port = 6000
    # while True:
    #     port += 1
    #     yield port

    # Assign two ports, one for the main server thread and the other for the heartbeat thread
    global _port
    _port += 2
    return _port


def launch_heartbeat_thread(app, address, dec_q):
    logger.info(f"Effis launching heartbeat server thread with heart rate of {app.heart_rate} seconds")
    t = Thread(target=_heartbeat_monitor, args=(app, address, dec_q))
    t.daemon = True
    t.start()
    return t

def _heartbeat_monitor(app, address, dec_q):
    """
    Target function for the heartbeat monitor thread

    Args:
    app (AppDef) : Object of class AppDef
    address (tuple) : (hostname, port) tuple to setup an incoming socket connection from the heartbeat client thread
    dec_q (queue) : Queue to communicate with the decision engine
    """
    # Open connection with heartbeart thread on the app's side
    listener = Listener(address)
    conn = listener.accept()
    logger.info(f"{app.name} server hb thread established connection with hb client")

    # Monitor for heart beat from the heartbeat thread on the client side
    hb_found = True
    while hb_found:
        hb_found = conn.poll(timeout=app.heart_rate)
        if not hb_found: break
        try:
            hb = conn.recv()
        except EOFError:
            logger.debug(f"Effis heartbeat monitor for {app.name} encountered EOFError with connection. "
                         f"Client thread seems to have terminated.")
            return
        logger.debug(f"Effis server thread received heartbeat from {app.name}")

    # Heartbeat not found within the specified heart_rate timeout. Notify the decision engine and return.
    logger.critical(f"Heartbeat not found within {app.heart_rate} seconds for {app.name}. "
                    f"Notifying decision engine")
    msg = signals.EFFIS_HEARTBEAT_NOT_DETECTED
    dec_q.put((msg, app))

def launch_server_thread(app_name, address, q, thread_type):
    logger.debug(f"{app_name} launching server thread on address {address}")
    t = Thread(target=server_thread, args=(app_name, address, q, thread_type))
    t.deamon = True
    t.start()

    hbt = None

    return (t, hbt)

