from threading import Thread, Queue
from mpi4py import MPI
from effis.app_client import effis_app_client
from effis.signals import EFFIS_CONTINUE, EFFIS_SIGTERM, CLIENT_READY, CLIENT_STOP


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

    # Create queue and spawn listener. Wait for listener to spawn up and indicate all is good.
    _q = Queue()
    _t = Thread(target = effis_app_client, args=(_q,))
    _t.start()

    # Get a READY signal from the thread which indicates the thread was able to connect
    # with the EFFIS server successfully
    signal = _q.get()
    assert signal == CLIENT_READY, f"Expected {CLIENT_READY}, received {signal} from app client f{app_name}"

    _checkpoint_routine = checkpoint_routine
    _cleanup_routine = cleanup_routine


def effis_check(checkpoint_args = (), cleaup_args = ()):
    rank = MPI.COMM_WORLD.Get_rank()
    signal = None

    # check queue if a signal has been received
    if rank == 0:
        try:
            _q.get(block=False)
        except Queue.Empty:
            # nothing in queue. tell everyone to proceed.
            signal = EFFIS_CONTINUE
    

    # Handle signals
    signal = MPI.COMM_WORLD.bcast(signal)
    if signal == EFFIS_CONTINUE:
        return

    # Handle signal to safely terminate the application
    elif signal == EFFIS_SIGTERM:
        if rank == 0:
            # Stop the app client
            q.put(CLIENT_STOP)
            _t.join()
        _checkpoint_routine(*checkpoint_args)
        _cleanup_routine(*cleanup_args)

    else:
        raise Exception(f"Received unknown signal {signal} from app-client for {app_name}")

