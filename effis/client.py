from multiprocessing.connection import Client
import effis.signals as effis_signals


def effis_client(app_name, q):
    # Start connection with the effis server
    with open(f"{app_name}.conn_info") as f:
        conn_info = f.readline()
        hostname = conn_info.split(":")[0]
        port = int(conn_info.split(":")[1])
    
    address = (hostname, port)
    conn = Client(address)

    # Send an ack that I am ready
    conn.send(effis_signals.CLIENT_READY)

    # Tell the application that I am ready
    q.put(effis_signals.CLIENT_READY)

    # Now start monitoring for a signal
    if app_name == 'Analysis':
        # Get signal from the application
        signal = q.get()

        # forward the signal to the effis server
        conn.send(signal)

    else:
        # Receive signal
        signal = conn.recv()

        # Forward it to the application
        q.put(signal)
