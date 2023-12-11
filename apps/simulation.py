import adios2
from mpi4py import MPI
import os, sys, traceback
import time
from effis.api import effis_init, effis_check, effis_finalize


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
        me = f"{os.path.basename(sys.argv[0])}:{sys.argv[1]}"
        rank = MPI.COMM_WORLD.Get_rank()

        nt = 10
        icheckpoint = 0
        ad2 = adios2.ADIOS()

        # Just write a string value from the root for now
        if rank == 0:

            writer, checkpoint_engine, v1, v2 = init_adios_objects(ad2)

            effis_init(os.path.basename(sys.argv[0]), checkpoint, cleanup)

            # Begin timestepping
            for t in range(nt):
                if rank == 0:
                    print(f"{me} starting timestep {t}", flush=True)
                time.sleep(0.1)
                writer.BeginStep()
                writer.Put(v1, f"Timestep {t} from {me}, P{rank}")
                writer.EndStep()

                if t % 3 == 0 and t != 0:
                    icheckpoint += 1
                    checkpoint(checkpoint_engine, v2, icheckpoint, t)

                effis_check(checkpoint_args = (checkpoint_engine, v2, icheckpoint, t),
                            cleanup_args = (writer, checkpoint_engine))
         
            cleanup(writer, checkpoint_engine)
            effis_finalize()

    except Exception as e:
        print(e)
        print(traceback.format_exc())


if __name__ == '__main__':
    main()
