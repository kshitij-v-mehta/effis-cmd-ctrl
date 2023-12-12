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
    
    writer            = io.Open   ("test.bp",       adios2.Mode.Write, MPI.COMM_SELF)
    checkpoint_engine = io_c.Open ("checkpoint.bp", adios2.Mode.Write, MPI.COMM_SELF)

    return writer, checkpoint_engine, v1, v2


def main():
    try:
        assert len(sys.argv) == 2
        app_name = f"{os.path.basename(sys.argv[0])}:{sys.argv[1]}"
        rank = MPI.COMM_WORLD.Get_rank()

        nt = 10
        icheckpoint = 0
        ad2 = adios2.ADIOS()

        # Just write a string value from the root for now
        if rank == 0:

            writer, checkpoint_engine, v1, v2 = init_adios_objects(ad2)
            logger.info(f"{app_name} initialized adios. Now calling effis_init")

            effis_init(os.path.basename(sys.argv[0]), checkpoint, cleanup)

            # Begin timestepping
            logger.info(f"{app_name} starting timestepping")
            for t in range(nt):
                if rank == 0:
                    logging.info(f"{app_name} starting timestep {t}")
                time.sleep(0.1)
                writer.BeginStep()
                writer.Put(v1, f"Timestep {t} from {app_name}, P{rank}")
                writer.EndStep()

                if t % 3 == 0 and t != 0:
                    icheckpoint += 1
                    checkpoint(checkpoint_engine, v2, icheckpoint, t)

                logger.info(f"{app_name} calling effis_check")
                effis_check(checkpoint_args = (checkpoint_engine, v2, icheckpoint, t),
                            cleanup_args = (writer, checkpoint_engine))
         
            logger.info(f"{app_name} calling cleanup")
            cleanup(writer, checkpoint_engine)
            
            logger.info(f"{app_name} calling effis_finalize")
            effis_finalize()
            
            logger.info(f"{app_name} done. Exiting.")

    except Exception as e:
        print(e)
        print(traceback.format_exc())


if __name__ == '__main__':
    main()
