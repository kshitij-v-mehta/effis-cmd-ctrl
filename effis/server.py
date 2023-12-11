from multiprocessing.connection import Listener
from threading import Thread
import socket
import effis.signals as signals

_port = 6000


def server_thread(port, app_name, q):
    # Write connection info for the client to connect
    with open(f"{app_name}.conn_info", "w") as f:
        f.write(f"{socket.gethostname()}:{port}")

    # Start listening for connections
    addr = (socket.gethostname(), port)
    listener = Listener(addr)
    conn = listener.accept()

    # Recv ack from client that it is ready
    msg = conn.recv()
    assert msg == signals.CLIENT_READY, f"EFFIS server thread for {app_name} received unknown acknowledgement {msg} instead of {signals.CLIENT_READY}"

    # Wait for a message
    msg = None
    if app_name == "Analysis":
        # receive message from analysis client and forward it to the application
        msg = conn.recv()
        q.put(msg)
    else:
        # extract signal from the queue from the analysis server and forward it to the simulation client
        msg = q.get()
        conn.send(msg)

    # Close after you received a message
    conn.close()


def launch_server_thread(app_name, q):
    port = _port
    t = Thread(target=server_thread, args=(port, app_name, q))
    t.start()
    _port += 1

    return t

