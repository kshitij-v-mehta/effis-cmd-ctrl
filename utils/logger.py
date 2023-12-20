import logging
from mpi4py import MPI


class MyLogger(logging.Logger):
    def __init__(self, name, level=logging.DEBUG):
        return super().__init__(name, level)


    def info(self, msg, *args, **kwargs):
        if MPI.COMM_WORLD.Get_rank() == 0:
            return super().info(msg, *args, **kwargs)


logging.setLoggerClass(MyLogger)
logging.basicConfig(level=logging.DEBUG, format="%(levelname)s | %(asctime)s | %(filename)16s:%(funcName)32s():%(lineno)3s | %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")

logger = logging.getLogger("effis-cmd-ctrl")


if __name__ == '__main__':
    logger.info("Test")
