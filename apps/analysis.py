import adios2
from mpi4py import MPI
import os, sys
import effis.signals as effis_signals
from effis.api import effis_init, effis_finalize, effis_signal
from utils.logger import logger


def main():
    # Only root
    if MPI.COMM_WORLD.Get_rank() != 0:
        return

    app_name = os.path.basename(sys.argv[0])

    # Start effis socket thread
    logger.info(f"{app_name} calling effis_init")
    effis_init(os.path.basename(app_name))

    ad2 = adios2.ADIOS()
    io = ad2.DeclareIO("reader")
    io.SetEngine("BP5")
    engine = io.Open("test.bp", adios2.Mode.Read)
    logger.info(f"{app_name} opened test.bp for reading")
    
    while(engine.BeginStep() == adios2.StepStatus.OK):
        ad_var = io.InquireVariable("Str")
        v = engine.Get(ad_var)

        logger.info(f"{app_name} read next step. Value: {v}")

        # Indicate error condition
        if 'Timestep 5' in v:
            logger.info(f"{app_name} detected condition. Sending signal")
            effis_signal(effis_signals.EFFIS_SIGTERM)
            engine.EndStep()
            break

        engine.EndStep()
    engine.Close()
    logger.info(f"{app_name} closed test.bp")

    # Shut the socket thread
    logger.info(f"{app_name} calling effis_finalize")
    effis_finalize()

    logger.info(f"{app_name} done. Exiting.")


if __name__ == '__main__':
    main()
