from multiprocessing.connection import Client
import socket, time, sys


def main():
    assert len(sys.argv) == 2
    port = 6000 + int(sys.argv[1])
    hostid = socket.gethostname().split(".olcf.ornl.gov")[0]
   
    print(f"App client running on {socket.gethostname()}, ip addr: {socket.gethostbyname(socket.gethostname())}, port: {port}", flush=True)
    
    with open("receiver-hostname.txt") as f:
        receiver_hostname = f.readline()
    
    address = (receiver_hostname, port)
    conn = Client(address, authkey = b'secretpassword')
    
    rcv_msg = ""
    for msg in [f'hello_{hostid}', f'close', f'hello_again_{hostid}']:
        time.sleep(1)
        conn.send(msg)
        rcvmsg = conn.recv()
        print(f"{socket.gethostname()} received {rcvmsg}")
        if rcvmsg == 'close':
            print(f"{socket.gethostname()} shutting down.")
            break
    conn.close()


if __name__ == '__main__':
    main()

