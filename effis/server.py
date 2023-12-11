from multiprocessing.connection import Listener
from threading import Thread
import socket
import effis.signals as signals


_port = 6000


def server_thread(port, app_name, q):
    addr = (socket.gethostname(), port)
    listener = Listener(address)
    conn = listener.accept()

    msg = conn.recv()
    assert msg == signals.CLIENT_READY, f"EFFIS server thread for {app_name} received unknown acknowledgement {msg} instead of {signals.CLIENT_READY}"

    msg = None
    if app_name == "Analysis":
        msg = conn.recv()
        q.put(msg)
    else:
        msg = q.get()
        conn.send(msg)

    conn.close()


def launch_server_thread(app_name, q):
    port = _port
    t = Thread(target=server_thread, args=(port, app_name, q))
    t.start()
    _port += 1

    return t

