from threading import Thread
from queue import Queue, Empty
from mpi4py import MPI
from effis.client import effis_client
from effis.signals import EFFIS_CONTINUE, EFFIS_SIGTERM, CLIENT_READY, CLIENT_DONE
from utils.logger import logger


_checkpoint_routine = None
_cleanup_routine = None
_q = None
_t = None
_app_name = None


def effis_init(app_name, checkpoint_routine=None, cleanup_routine=None):
    global _q, _t, _app_name, _checkpoint_routine, _cleanup_routine
    _app_name = app_name

    _checkpoint_routine = checkpoint_routine
    _cleanup_routine = cleanup_routine

    # Only root creates the app listener. Everyone else returns.
    if MPI.COMM_WORLD.Get_rank() != 0:
        return

    logger.info(f"{app_name} ready to create effis client thread")
    # Create queue and spawn listener. Wait for listener to spawn up and indicate all is good.
    _q = Queue()
    _t = Thread(target=effis_client, args=(app_name, _q,))
    _t.start()

    # Get a READY signal from the thread which indicates the thread was able to connect
    # with the EFFIS server successfully
    logger.info(f"{app_name} waiting for ready signal from client thread")
    signal = _q.get()
    _q.task_done()
    assert signal == CLIENT_READY, f"Expected {CLIENT_READY}, received {signal} from app client f{app_name}"

    logger.info(f"{app_name} received ready signal from client thread. Exiting effis_init.")


def effis_check(checkpoint_args = (), cleanup_args = ()):
    rank = MPI.COMM_WORLD.Get_rank()
    signal = None
    retval = 0

    # check queue if a signal has been received
    if rank == 0:
        try:
            logger.info(f"{_app_name} checking to see if a signal has been delivered.")
            signal = _q.get(block=False)
            _q.task_done()
        except Empty:
            # nothing in queue. tell everyone to proceed.
            signal = EFFIS_CONTINUE
    
    # Handle signals
    if rank == 0: logger.info(f"{_app_name} Broadcasting signal {signal}.")
    signal = MPI.COMM_WORLD.bcast(signal)
    if signal == EFFIS_CONTINUE:
        return

    # Handle signal to safely terminate the application
    elif signal == EFFIS_SIGTERM:
        logger.info(f"{_app_name} received {signal}. Performing checkpoint.")
        _checkpoint_routine(*checkpoint_args)
        
        if _cleanup_routine is not None:
            logger.info(f"{_app_name} performing cleanup.")
            _cleanup_routine(*cleanup_args)

            logger.info(f"{_app_name} performed cleanup.")

        if rank == 0:
            logger.info(f"{_app_name} waiting for client thread to terminate")
            # Stop the app client
            _t.join()

        retval = 1

    elif signal == CLIENT_DONE:
        logger.info(f"{_app_name} received {signal}.")

    else:
        raise Exception(f"{_app_name} received unknown signal {signal} from app-client")

    return retval


def effis_signal(signal):
    if MPI.COMM_WORLD.Get_rank() != 0:
        return

    global _app_name
    logger.info(f"{_app_name} putting {signal} in queue")
    _q.put(signal)


def effis_finalize():
    if MPI.COMM_WORLD.Get_rank() != 0:
        return

    effis_signal(CLIENT_DONE)
    _t.join()

