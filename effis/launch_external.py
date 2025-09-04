import subprocess
from utils.logger import logger


class AppDef:
    def __init__(self, name, exe, input_args, nprocs, ppn, num_nodes, cpus_per_task, gpus_per_task, tau_profiling,
                 working_dir, mpi=True, monitor_heartbeat = False, heart_rate=None):
        self.name = name
        self.exe = exe
        self.input_args = input_args
        self.nprocs = nprocs
        self.ppn = ppn
        self.num_nodes = num_nodes
        self.cpus_per_task = cpus_per_task
        self.gpus_per_task = gpus_per_task
        self.working_dir = working_dir
        self.mpi = mpi
        self.tau_profiling = tau_profiling
        self.monitor_heartbeat = monitor_heartbeat
        self.heart_rate = heart_rate


def form_mpi_cmd(app):
    run_cmd = f"mpirun -np {app.nprocs} python3 {app.exe}"
    return run_cmd.split()


def launch_external(app: AppDef):
    run_cmd = form_mpi_cmd(app)
    logger.debug("Effis launching external app %s", run_cmd)
    p = None
    try:
        p = subprocess.Popen(run_cmd, cwd=app.working_dir)
    except subprocess.CalledProcessError as e:
        logger.error(f"Error {e} launching external app")
    return p
