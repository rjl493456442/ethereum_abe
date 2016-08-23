#! /usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
from abe import flags
from logging.handlers import RotatingFileHandler

FLAGS = flags.FLAGS


def init_log(log_name):
    
    logger = logging.getLogger(log_name)
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s <%(name)s>: %(message)s '
        '[in %(pathname)s:%(lineno)d]')
    #logging.basicConfig(filemode = 'a')

    level = FLAGS.log_level
    if level.lower() == "error":

        error_log = os.path.join('.', FLAGS.ERROR_LOG)
        error_file_handler = RotatingFileHandler(error_log, backupCount=10)
        error_file_handler.setLevel(logging.ERROR)
        error_file_handler.setFormatter(formatter)
        logger.addHandler(error_file_handler)
        logger.setLevel(logging.ERROR)

    elif level.lower() == "info":

        info_log = os.path.join('.', FLAGS.INFO_LOG)
        info_file_handler = RotatingFileHandler(info_log, backupCount=10)
        info_file_handler.setLevel(logging.INFO)
        info_file_handler.setFormatter(formatter)
        logger.addHandler(info_file_handler)
        logger.setLevel(logging.INFO)

    else:
        # default is error
        debug_log = os.path.join('.', FLAGS.DEBUG_LOG)
        debug_file_handler = RotatingFileHandler(debug_log, backupCount=10)
        debug_file_handler.setLevel(logging.DEBUG)
        debug_file_handler.setFormatter(formatter)
        logger.addHandler(debug_file_handler)
        logger.setLevel(logging.DEBUG)

    return logger



