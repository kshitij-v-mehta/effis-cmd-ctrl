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


def effis_init(app_name, checkpoint_routine, cleanup_routine):
    _app_name = app_name

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
    assert signal == CLIENT_READY, f"Expected {CLIENT_READY}, received {signal} from app client f{app_name}"

    _checkpoint_routine = checkpoint_routine
    _cleanup_routine = cleanup_routine
    logger.info(f"{app_name} received ready signal from client thread. Exiting effis_init.")


def effis_check(checkpoint_args = (), cleanup_args = ()):
    rank = MPI.COMM_WORLD.Get_rank()
    signal = None

    # check queue if a signal has been received
    if rank == 0:
        try:
            logger.info(f"{_app_name} checking to see if a signal has been delivered.")
            _q.get(block=False)
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
        logger.info(f"{_app_name} received {signal}. Calling checkpoint and cleanup routines.")
        _checkpoint_routine(*checkpoint_args)
        
        logger.info(f"{_app_name} performed checkpoint. Now calling cleanup.")
        _cleanup_routine(*cleanup_args)

        logger.info(f"{_app_name} performed cleanup. Now waiting for client thread to terminate.")
        if rank == 0:
            # Stop the app client
            _t.join()

    elif signal == CLIENT_DONE:
        logger.info(f"{_app_name} received {signal}. Exiting effis_check.")
        pass

    else:
        raise Exception(f"{_app_name} received unknown signal {signal} from app-client")


def effis_signal(signal):
    _q.put(signal)


def effis_finalize():
    _t.join()

