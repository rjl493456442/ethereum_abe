import log
import flags
import os
import sys
FLAGS = flags.FLAGS
FLAGS(sys.argv)
logger = log.init_log("abe")
BASE_PATH = os.getcwd()
