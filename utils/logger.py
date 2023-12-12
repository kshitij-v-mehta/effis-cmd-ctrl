import logging


logger = logging.getLogger("effis-cmd-ctrl")
logging.basicConfig(level=logging.DEBUG, format="%(levelname)s | %(asctime)s | %(filename)16s:%(funcName)32s():%(lineno)s | %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")


if __name__ == '__main__':
    logger.info("Test")
