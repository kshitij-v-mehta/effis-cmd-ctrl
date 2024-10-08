import adios2
from mpi4py import MPI
import os, sys, traceback
import effis.signals as effis_signals
import effis.api as effis
from utils.logger import logger


def main():
    app_name = os.path.basename(sys.argv[0])
    rank = MPI.COMM_WORLD.Get_rank()

    # Start effis socket thread
    if rank==0: logger.info(f"{app_name} calling effis.init")
    effis.init(os.path.basename(app_name))

    ad2 = adios2.ADIOS()
    io = ad2.DeclareIO("reader")
    io.SetEngine("BP5")
    engine = io.Open("test.bp", adios2.Mode.Read, MPI.COMM_WORLD)
    if rank==0: logger.info(f"{app_name} opened test.bp for reading")
    
    while(engine.BeginStep() == adios2.StepStatus.OK):
        ad_var = io.InquireVariable("Str")
        v = engine.Get(ad_var)

        # Indicate error condition
        retval = 0
        cur_t = int(v.split("Timestep")[1].split(" from")[0].split("/")[0])
        total_t = int(v.split("Timestep")[1].split(" from")[0].split("/")[1]) 

        if rank==0: logger.info(f"{app_name} read next step. Value: '{v}'")

        if rank != 0:
            if cur_t > total_t // 2:
                retval = 1
        check = MPI.COMM_WORLD.allreduce(retval)
        if check > 0:
            if rank==0: logger.warning(f"{app_name} detected condition. Sending signal")
            effis.signal(effis_signals.EFFIS_SIGTERM)
            engine.EndStep()
            break

        engine.EndStep()
    engine.Close()
    if rank==0: logger.info(f"{app_name} closed test.bp")

    # Shut the socket thread
    if rank==0: logger.info(f"{app_name} calling effis.finalize")
    effis.finalize()

    if rank==0: logger.info(f"{app_name} done. Exiting.")


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(e)
        print(traceback.format_exc())

