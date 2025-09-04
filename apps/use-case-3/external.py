import adios2
from mpi4py import MPI
import os, sys, traceback
from utils.logger import logger


def main():
    app_name = os.path.basename(sys.argv[0])
    rank = MPI.COMM_WORLD.Get_rank()
    logger.info(f"{app_name} started")
    print(f"{app_name} started")

    ad2 = adios2.ADIOS()
    io = ad2.DeclareIO("reader")
    io.SetEngine("BP5")
    engine = io.Open("test.bp", adios2.Mode.Read, MPI.COMM_WORLD)
    if rank==0: logger.info(f"{app_name} opened test.bp for reading")
    
    while(engine.BeginStep() == adios2.StepStatus.OK):
        ad_var = io.InquireVariable("Str")
        v = engine.Get(ad_var)

        if rank==0: logger.info(f"{app_name} read next step. Value: '{v}'")

        engine.EndStep()
    engine.Close()
    if rank==0: logger.info(f"{app_name} closed test.bp")
    if rank==0: logger.info(f"{app_name} done. Exiting.")


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(e)
        print(traceback.format_exc())
