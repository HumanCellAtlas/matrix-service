import logging
import os
import sys


class Logging:
    @staticmethod
    def get_logger(name: str):
        ch = logging.StreamHandler(sys.stdout)
        log_level_name = os.environ['LOG_LEVEL'] if 'LOG_LEVEL' in os.environ else 'DEBUG'
        log_level = getattr(logging, log_level_name.upper())
        ch.setLevel(log_level)
        formatter = logging.Formatter('%(asctime)s %(thread)d %(levelname)s %(name)s %(message)s',
                                      datefmt="%Y-%m-%dT%H:%M:%S%z")
        ch.setFormatter(formatter)
        logger = logging.getLogger(name)
        logger.handlers = []
        logger.addHandler(ch)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        return logger

    @staticmethod
    def set_correlation_id(logger: logging.Logger, id_name: str="REQUEST_HASH", value: str=None):
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s' +
                                      f' {id_name}:{value} %(message)s', datefmt="%Y-%m-%dT%H:%M:%S%z")
        logger.handlers[0].setFormatter(formatter)
        return logger
