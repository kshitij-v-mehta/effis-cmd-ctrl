import adios2
from mpi4py import MPI
import os, sys, traceback
import time
from utils.logger import logger


def init_adios_objects(ad2):
    io  = ad2.DeclareIO("IO")
    io.SetEngine("BP5")
    v1 = io.DefineVariable("Str")
    
    writer = io.Open("test.bp", adios2.Mode.Write, MPI.COMM_WORLD)
    return writer, v1


def perform_computation(delay=1):
    time.sleep(delay)


def main():
    try:
        app_name = f"{os.path.basename(sys.argv[0])}"
        rank = MPI.COMM_WORLD.Get_rank()
        nt = 20

        program_timer = time.time()
        ad2 = adios2.ADIOS()
        writer, v1 = init_adios_objects(ad2)

        # Begin timestepping
        if rank==0: logger.info(f"{app_name} starting timestepping")
        for t in range(nt):
            if rank == 0:
                logger.info(f"{app_name} starting timestep {t}")

            # Science app performs some computations
            perform_computation()

            # Write a step of test.bp
            writer.BeginStep()
            writer.Put(v1, f"Timestep {t}/{nt-1} from {app_name}, P{rank}")
            writer.EndStep()
        writer.Close()
        
        if rank==0:
            logger.info(f"{app_name} total program time: {round(time.time()-program_timer, 2)} seconds")
        logger.info(f"Rank {rank} of {app_name} done. Exiting.")

    except Exception as e:
        print(e)
        print(traceback.format_exc())


if __name__ == '__main__':
    main()
