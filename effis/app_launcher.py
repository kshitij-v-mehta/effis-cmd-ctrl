import subprocess
from effis.server import launch_server_thread
from queue import Queue


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
    run_cmd = f"srun --exclusive -n {app.nprocs} --ntasks-per-node={app.ppn} --cpus-per-task={app.cpus_per_task} {app.exe}"
    return run_cmd.split()


def _launch(app):
    _server_threads.append(launch_server_thread(app.name, _q))
    run_cmd = form_slurm_cmd(app)
    p = subprocess.Popen(run_cmd, cwd=app.working_dir)
    return p


def _launch_apps():
    simulation = _App(name='Simulation', 
                      exe="/Users/kpu/vshare/effis-cmd-ctrl/apps/simulation.py", 
                      input_args = (), nprocs=1, ppn=1, cpus_per_task=1, gpus_per_task=None,
                      working_dir="/Users/kpu/vshare/effis-cmd-ctrl/test-dir")

    analysis   = _App(name='Analysis', 
                      exe="/Users/kpu/vshare/effis-cmd-ctrl/apps/anslysis.py", 
                      input_args = (), nprocs=1, ppn=1, cpus_per_task=1, gpus_per_task=None,
                      working_dir="/Users/kpu/vshare/effis-cmd-ctrl/test-dir")

    _apps_running.append(_launch(simulation))
    _apps_running.append(_launch(analysis))
    

def _monitor_queue():
    for t in _server_threads:
        t.join()


def _main():
    _launch_apps()
    _monitor_queue()


if __name__ == '__main__':
    _main()

