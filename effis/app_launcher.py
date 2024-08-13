import os, subprocess, socket
from effis.server import launch_server_thread, launch_heartbeat_thread, get_port
from queue import Queue
from utils.logger import logger


_apps_running = []
_server_threads = []
_q = Queue()
_dec_q = Queue()  # Queue to communicate with the decision engine


class RunningApp:
    def __init__(self, p, appdef, threadpair, dec_q):
        """
        Representing an app that is running.

        Args:
        appdef     (class _AppDef) : an object of class _AppDef
        p          (subprocess)    : The process object of the running app
        threadpair (tuple)         : a tuple of a pair of Threads (server thread, heartbeat thread)
        dec_q      (queue)         : a queue to communicate with the decision engine
        """
        self.appdef = appdef
        self.p = p
        self.threadpair = threadpair


class _AppDef:
    def __init__(self, name, exe, input_args, nprocs, ppn, num_nodes, cpus_per_task, gpus_per_task, tau_profiling, working_dir, monitor_heartbeat = False, heart_rate=None):
        self.name = name
        self.exe = exe
        self.input_args = input_args
        self.nprocs = nprocs
        self.ppn = ppn
        self.num_nodes = num_nodes
        self.cpus_per_task = cpus_per_task
        self.gpus_per_task = gpus_per_task
        self.working_dir = working_dir
        self.tau_profiling = tau_profiling
        self.monitor_heartbeat = monitor_heartbeat
        self.heart_rate = heart_rate


def form_slurm_cmd(app):
    if app.tau_profiling:
        run_cmd = f"srun -n {app.nprocs} -N {app.num_nodes} --ntasks-per-node={app.ppn} --cpus-per-task={app.cpus_per_task} tau_exec python3 {app.exe}"
    else:
        run_cmd = f"srun -n {app.nprocs} -N {app.num_nodes} --ntasks-per-node={app.ppn} --cpus-per-task={app.cpus_per_task} python3 {app.exe}"
    return run_cmd.split()


def form_mpi_cmd(app):
    run_cmd = f"mpirun -np {app.nprocs} python3 {app.exe}"
    return run_cmd.split()


def _launch(app):
    global _dec_q
    logger.debug(f"{app.name} launching server thread")
    thread_type = 'listener'
    if 'analysis' in app.name:
        thread_type = 'sender'
    port = get_port()
    address = (socket.gethostname(), port)
    st = launch_server_thread(app.name, address, _q, thread_type)
    _server_threads.append(st)
    # run_cmd = form_slurm_cmd(app)
    run_cmd = form_mpi_cmd(app)
    
    logger.info(f"{app.name} launching application as {run_cmd}")
    env = os.environ
    if app.tau_profiling:
        env['TAU_PROFILE'] = "1"
        env['PROFILE_DIR'] = os.path.join(app.working_dir, 'tau-profile')
        logger.debug(f"{app.name} Added tau profiling to env")
    p = subprocess.Popen(run_cmd, cwd=app.working_dir, env=env)

    # Launch heartbeat thread if heartbeat monitoring is enabled
    hbt = None
    if app.monitor_heartbeat:
        assert app.heart_rate is not None, "Need a valid value for {app.name}'s heart rate"
        launch_heartbeat_thread(app, (socket.gethostname(), port+1), _dec_q)

    return (RunningApp(p, app, (st, hbt), _dec_q))

def _launch_apps():
    root = "/home/kmehta/vshare/effis-cmd-ctrl/apps"
    sim_num_nodes = int(os.getenv("SLURM_JOB_NUM_NODES") or 0)-1
    simulation = _AppDef(name='simulation.py', 
                         exe=f"{root}/simulation.py", 
                         input_args = (), nprocs=2, ppn=2, num_nodes=1, 
                         cpus_per_task=1, gpus_per_task=None, 
                         tau_profiling=False, working_dir=os.getcwd(),
                         monitor_heartbeat=True, heart_rate=2.0)

    analysis   = _AppDef(name='analysis_2.py', 
                         exe=f"{root}/analysis_2.py", 
                         input_args = (), nprocs=2, ppn=2, num_nodes=1, cpus_per_task=1, gpus_per_task=None,
                         tau_profiling=False, working_dir=os.getcwd())

    _apps_running.append(_launch(simulation))
    _apps_running.append(_launch(analysis))
    

def _start_decision_engine():
    pass

def _wait():
    logger.debug(f"Waiting for server threads to finish")
    for t in _server_threads:
        t[0].join()

def _main():
    _launch_apps()
    _start_decision_engine()
    _wait()


if __name__ == '__main__':
    _main()

