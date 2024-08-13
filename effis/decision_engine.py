from threading import Thread
import sys, time, os, signal
from utils.logger import logger
import effis.signals as signals


def _terminate_workflow(l_apps):
    """
    Terminate the entire workflow by terminating all applications.
    All server and client threads will exit automatically as they are daemon threads.

    Args:
    l_apps (list) : list of RunningApp objects
    """
    # Send all server threads to send EFFIS_SIGTERM to their client counterparts

    # Send everyone SIGTERM first
    for running_app in l_apps:
        pid = running_app.pid
        pid.terminate()

    # Give everyone the time to terminate cleanly
    time.sleep(5)

    # Send everyone SIGKILL. It will kill whoever has not terminated yet.
    for running_app in l_apps:
        pid = running_app.pid
        pid.kill()

    logger.info(f"Effis has terminated all workflow components.")

def _decision_engine(dec_q, server_q, l_apps):
    """
    Target function for the decision engine thread. The fate of the workflow will be decided here!

    Args:
    dec_q (queue) : A queue where other effis threads (e.g. heartbeat threads) will send messages
    server_q (queue) : Queue for all effis server threads
    l_apps (list) : A list of RunningApp objects
    """
    msg_t = dec_q.get()
    msg = msg_t[0]
    app = msg_t[1]

    if msg == 'QUIT': return

    if msg == signals.EFFIS_HEARTBEAT_NOT_DETECTED:
        logger.critical(f"Effis decision engine notified heartbeat not detected for {app.name}. "
                        f"Terminating workflow")
        _terminate_workflow(l_apps)
        server_q.put(signals.EFFIS_QUIT)
    
    logger.info("Effis decision engine now terminating Effis. Goodbye ..")
    os.kill(os.getpid(), signal.SIGTERM)
    sys.exit()

def launch_decision_engine(dec_q, server_q, l_apps):
    t = Thread(target=_decision_engine, args=(dec_q, server_q, l_apps))
    t.daemon=True
    t.start()
    return t

def terminate_decision_engine(dec_q):
    dec_q.put( ('QUIT', None) )

