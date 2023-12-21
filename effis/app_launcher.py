import os, subprocess
from effis.server import launch_server_thread
from queue import Queue
from utils.logger import logger


_apps_running = []
_server_threads = []
_q = Queue()


class _App:
    def __init__(self, name, exe, input_args, nprocs, ppn, num_nodes, cpus_per_task, gpus_per_task, tau_profiling, working_dir):
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
    logger.debug(f"{app.name} launching server thread")
    thread_type = 'listener'
    if 'analysis' in app.name:
        thread_type = 'sender'
    _server_threads.append(launch_server_thread(app.name, _q, thread_type))
    run_cmd = form_slurm_cmd(app)
    # run_cmd = form_mpi_cmd(app)
    
    logger.info(f"{app.name} launching application as {run_cmd}")
    env = os.environ
    if app.tau_profiling:
        env['TAU_PROFILE'] = "1"
        env['PROFILE_DIR'] = os.path.join(app.working_dir, 'tau-profile')
        logger.debug(f"{app.name} Added tau profiling to env")
    p = subprocess.Popen(run_cmd, cwd=app.working_dir, env=env)
    return p


def _launch_apps():
    sim_num_nodes = int(os.getenv("SLURM_JOB_NUM_NODES") or 0)-1
    simulation = _App(name='simulation.py', 
                      exe="/lustre/orion/csc143/world-shared/kmehta/effis-cmd-ctrl/apps/simulation.py", 
                      input_args = (), nprocs=32*sim_num_nodes, ppn=32, num_nodes=sim_num_nodes, 
                      cpus_per_task=1, gpus_per_task=None, 
                      tau_profiling=False, working_dir=os.getcwd())

    analysis   = _App(name='analysis.py', 
                      exe="/lustre/orion/csc143/world-shared/kmehta/effis-cmd-ctrl/apps/analysis.py", 
                      input_args = (), nprocs=32, ppn=32, num_nodes=1, cpus_per_task=1, gpus_per_task=None,
                      tau_profiling=False, working_dir=os.getcwd())

    _apps_running.append(_launch(simulation))
    _apps_running.append(_launch(analysis))
    

def _monitor_queue():
    logger.debug(f"Waiting for server threads to finish")
    for t in _server_threads:
        t.join()


def _main():
    _launch_apps()
    _monitor_queue()


if __name__ == '__main__':
    _main()

