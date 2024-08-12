import os, logging


logger = logging.getLogger("effis-cmd-ctrl")
logging.basicConfig(level=os.environ.get("EFFIS_LOGLEVEL", logging.INFO), format="%(levelname)10s | %(asctime)s | %(filename)16s:%(funcName)32s():%(lineno)3s | %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")


if __name__ == '__main__':
    logger.info("Test")
