import subprocess
from effis.server import launch_server_thread
from queue import Queue
from utils.logger import logger


_apps_running = []
_server_threads = []
_q = Queue()


class _App:
    def __init__(self, name, exe, input_args, nprocs, ppn, cpus_per_task, gpus_per_task, working_dir):
        self.name = name
        self.exe = exe
        self.input_args = input_args
        self.nprocs = nprocs
        self.ppn = ppn
        self.cpus_per_task = cpus_per_task
        self.gpus_per_task = gpus_per_task
        self.working_dir = working_dir


def form_slurm_cmd(app):
    run_cmd = f"srun --exclusive -n {app.nprocs} --ntasks-per-node={app.ppn} --cpus-per-task={app.cpus_per_task} python3 {app.exe}"
    return run_cmd.split()


def form_mpi_cmd(app):
    run_cmd = f"mpirun -np {app.nprocs} python3 {app.exe}"
    return run_cmd.split()


def _launch(app):
    logger.info(f"{app.name} launching server thread")
    thread_type = 'listener'
    if 'analysis' in app.name:
        thread_type = 'sender'
    _server_threads.append(launch_server_thread(app.name, _q, thread_type))
    # run_cmd = form_slurm_cmd(app)
    run_cmd = form_mpi_cmd(app)
    
    logger.info(f"{app.name} launching application as {run_cmd}")
    p = subprocess.Popen(run_cmd, cwd=app.working_dir)
    return p


def _launch_apps():
    simulation = _App(name='simulation.py', 
                      exe="/home/kmehta/vshare/effis-cmd-ctrl/apps/simulation.py", 
                      input_args = (), nprocs=2, ppn=1, cpus_per_task=1, gpus_per_task=None,
                      working_dir="/home/kmehta/vshare/effis-cmd-ctrl/test-dir")

    analysis   = _App(name='analysis.py', 
                      exe="/home/kmehta/vshare/effis-cmd-ctrl/apps/analysis.py", 
                      input_args = (), nprocs=2, ppn=1, cpus_per_task=1, gpus_per_task=None,
                      working_dir="/home/kmehta/vshare/effis-cmd-ctrl/test-dir")

    _apps_running.append(_launch(simulation))
    _apps_running.append(_launch(analysis))
    

def _monitor_queue():
    logger.info(f"Waiting for server threads to finish")
    for t in _server_threads:
        t.join()


def _main():
    _launch_apps()
    _monitor_queue()


if __name__ == '__main__':
    _main()

