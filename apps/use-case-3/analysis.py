import adios2
import os, sys, traceback
import effis.api as effis
from utils.logger import logger
import effis
from mpi4py import MPI


def main():
    app_name = os.path.basename(sys.argv[0])
    rank = MPI.COMM_WORLD.Get_rank()

    # Start effis socket thread
    logger.info(f"{app_name} calling effis_init")
    effis.api.init(os.path.basename(sys.argv[0]))

    ad2 = adios2.ADIOS()
    io = ad2.DeclareIO("reader")
    io.SetEngine("BP5")
    engine = io.Open("test.bp", adios2.Mode.Read)
    logger.info(f"{app_name} opened test.bp for reading")

    analysis_process = None
    while(engine.BeginStep() == adios2.StepStatus.OK):
        check = 0
        ad_var = io.InquireVariable("Str")
        v = engine.Get(ad_var)

        # Indicate error condition
        cur_t = int(v.split("Timestep")[1].split(" from")[0].split("/")[0])
        total_t = int(v.split("Timestep")[1].split(" from")[0].split("/")[1])
        logger.info(f"{app_name} read next step. Value: '{v}'")

        if cur_t == total_t // 2:
            check = 1

        # Some condition in the data stream was detected. Launch external analysis task.
        if check > 0:
            logger.warning(f"{app_name} detected condition. Signaling effis to launching external analysis.")
            effis.api.signal("EFFIS_LAUNCH_EXTERNAL")
        engine.EndStep()
    engine.Close()

    logger.info(f"{app_name} closed test.bp. Now waiting for analysis task to complete.")
    # if analysis_process is not None:
    #     analysis_process.wait()

    logger.info(f"{app_name} calling effis finalize")
    effis.api.finalize()
    logger.info(f"{app_name} done. Exiting.")


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(e)
        print(traceback.format_exc())
