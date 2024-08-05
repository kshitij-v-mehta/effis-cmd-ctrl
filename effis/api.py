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

    logger.debug(f"{app_name} ready to create effis client thread")
    # Create queue and spawn listener. Wait for listener to spawn up and indicate all is good.
    thread_type = 'listener' if 'analysis' in app_name else 'sender'
    _q = Queue()
    _t = Thread(target=effis_client, args=(app_name, _q, thread_type))
    _t.start()

    logger.debug(f"{app_name} exiting effis_init.")


def effis_check(checkpoint_args = (), cleanup_args = (), signal_callbacks: dict = None):
    """
    Check if a signal has been delivered.
    The effis server spawned by the root process check for a signal. If found, it broadcasts to other processes, else
    broadcasts EFFIS_CONTINUE.
    Signal may be one of effis-defined signals (EFFIS_CONTINUE, EFFIS_SIGTERM etc.) or a user-defined signal in
    signal_callbacks.
    signal_callbacks contains a callback function and associated arguments for a signal.
    """
    rank = MPI.COMM_WORLD.Get_rank()
    signal = None
    retval = 0

    # check queue if a signal has been received
    if rank == 0:
        try:
            logger.debug(f"{_app_name} checking to see if a signal has been delivered.")
            signal = _q.get(block=False)
            _q.task_done()
        except Empty:
            # nothing in queue. tell everyone to proceed.
            signal = EFFIS_CONTINUE
    
    # Broadcast signal to all processes
    if rank == 0: logger.debug(f"{_app_name} Broadcasting signal {signal}.")
    signal = MPI.COMM_WORLD.bcast(signal)

    # Verify that a valid signal was received
    valid_signals = [EFFIS_CONTINUE, EFFIS_SIGTERM, CLIENT_DONE]
    if signal_callbacks is not None:
        valid_signals.extend(list(signal_callbacks.keys()))

    if signal not in valid_signals:
        raise Exception(f"{_app_name} received unknown signal {signal} from app-client.\nValid signals: {valid_signals}")

    # Nothing to do. Return to main app
    if signal == EFFIS_CONTINUE:
        return

    # Terminate the application safely
    elif signal == EFFIS_SIGTERM:
        if _checkpoint_routine is not None:
            if rank==0: logger.warning(f"{_app_name} received {signal}. Performing checkpoint.")
            _checkpoint_routine(*checkpoint_args)
        
        if _cleanup_routine is not None:
            logger.info(f"{_app_name} performing cleanup.")
            _cleanup_routine(*cleanup_args)
            logger.info(f"{_app_name} performed cleanup.")

        if rank == 0:
            logger.debug(f"{_app_name} waiting for client thread to terminate")
            # Stop the app client
            _t.join()

        retval = 1

    # Return
    elif signal == CLIENT_DONE:
        if rank==0: logger.debug(f"{_app_name} received {signal}.")

    # Received a user-defined signal from the app
    else:
        assert signal_callbacks is not None, "signal_callbacks cannot be None here"
        assert signal in list(signal_callbacks.keys()), f"Expected one of {list(signal_callbacks.keys())}, " \
                                                        f"found {signal}"

        signal_params = signal_callbacks[signal]
        callback = signal_params['cb']
        callback_args = signal_params['args']

        if rank==0: logger.info(f"effis_check received {signal}. Executing callback {callback}")
        callback(callback_args)

    return retval


def effis_signal(signal):
    if MPI.COMM_WORLD.Get_rank() != 0:
        return

    global _app_name
    logger.debug(f"{_app_name} putting {signal} in queue")
    _q.put(signal)


def effis_finalize():
    if MPI.COMM_WORLD.Get_rank() != 0:
        return

    effis_signal(CLIENT_DONE)
    _t.join()

