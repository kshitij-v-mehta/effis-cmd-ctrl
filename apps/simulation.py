import adios2
from mpi4py import MPI
import os, sys, traceback
import time
from effis.api import effis_init, effis_check, effis_finalize
from utils.logger import logger


def cleanup(writer, checkpoint_engine):
    writer.Close()
    checkpoint_engine.Close()


def checkpoint(adios2_writer, v, i, t):
    if MPI.COMM_WORLD.Get_rank()==0: logger.info("simulation main app performing checkpoint")
    adios2_writer.BeginStep()
    adios2_writer.Put(v, f"Checkpoint {i}, timestep {t}")
    adios2_writer.EndStep()


def init_adios_objects(ad2):
    io  = ad2.DeclareIO("IO")
    io.SetEngine("BP5")
    
    io_c = ad2.DeclareIO("Checkpoint")
    io_c.SetEngine("BP5")
    
    v1 = io.DefineVariable("Str")
    v2 = io_c.DefineVariable("Str")
    
    writer            = io.Open   ("test.bp",       adios2.Mode.Write, MPI.COMM_WORLD)
    checkpoint_engine = io_c.Open ("checkpoint.bp", adios2.Mode.Write, MPI.COMM_WORLD)

    return writer, checkpoint_engine, v1, v2


def perform_computation():
    time.sleep(1)


def main():
    try:
        app_name = f"{os.path.basename(sys.argv[0])}"
        rank = MPI.COMM_WORLD.Get_rank()

        nt = 20
        icheckpoint = 0
        ad2 = adios2.ADIOS()

        writer, checkpoint_engine, v1, v2 = init_adios_objects(ad2)

        if rank == 0:
            logger.info(f"{app_name} initialized adios. Now calling effis_init")
        
        effis_init(os.path.basename(sys.argv[0]), checkpoint, None)

        # Begin timestepping
        if rank==0: logger.info(f"{app_name} starting timestepping")
        for t in range(nt):
            if rank == 0:
                logger.info(f"{app_name} starting timestep {t}")

            perform_computation()

            writer.BeginStep()
            writer.Put(v1, f"Timestep {t}/{nt-1} from {app_name}, P{rank}")
            writer.EndStep()

            if t % 3 == 0 and t != 0:
                icheckpoint += 1
                checkpoint(checkpoint_engine, v2, icheckpoint, t)

            logger.debug(f"{app_name} calling effis_check")
            if 1 == effis_check(checkpoint_args = (checkpoint_engine, v2, icheckpoint, t),
                                cleanup_args = (writer, checkpoint_engine)):
                if rank==0: logger.warning(f"{app_name} app received 1 from effis_check. Terminating loop")
                break
        
        if rank==0: logger.info(f"{app_name} calling cleanup")
        cleanup(writer, checkpoint_engine)
        
        if rank==0: logger.info(f"{app_name} calling effis_finalize")
        effis_finalize()
        
        if rank==0: logger.info(f"{app_name} done. Exiting.")

    except Exception as e:
        print(e)
        print(traceback.format_exc())


if __name__ == '__main__':
    main()

