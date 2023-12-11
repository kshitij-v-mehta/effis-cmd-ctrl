from multiprocessing.connection import Listener
from multiprocessing import Process
from threading import Thread
import socket


def receive_msgs(conn):
    while(True):
        msg = conn.recv()
        print(f"Effis-server received {msg}", flush=True)
        if msg == 'close':
            print("Effis-server shutting down.")
            conn.close()
            break


def listener_process(port):
    address = (socket.gethostname(), port)
    listener = Listener(address, authkey=b'secretpassword')
    conn = listener.accept()
    print(f"Effis server: Connection accepted from {listener.last_accepted}", flush=True)
    
    while True:
        msg = conn.recv()
        print(f"Effis-server received {msg}", flush=True)
        conn.send(msg)
        if msg == 'close':
            conn.close()
            break


def main():
    print(f"Effis-server running on {socket.gethostname()}", flush=True)
    
    with open("receiver-hostname.txt", "w") as f:
        f.write(socket.gethostname())
 
    listeners = []   
    for port in [6001, 6002]:
        p = Thread(target=listener_process, args=(port,))
        p.start()
        listeners.append(p)
    
    for p in listeners:
        p.join()


if __name__ == '__main__':
    main()

