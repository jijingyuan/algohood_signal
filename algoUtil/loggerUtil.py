# -*- coding: utf-8 -*-
"""
@Create: 2024/9/10 10:46
@File: loggerUtil.py
@Author: Jingyuan
"""

import logging.config
import os
import datetime


def add_handler(name, logging, logdir, filedict):
    for i in filedict:
        logging.get('handlers').update({name + '_' + i: {
            "class": "logging.handlers.RotatingFileHandler",
            "level": i.upper(),
            "formatter": "standard",
            "filename": os.path.join(logdir, filedict[i]),
            'mode': 'w+',
            "maxBytes": 1024 * 1024 * 20,  # 20 MB
            "backupCount": 20,
            "encoding": "UTF-8"
        }})
    return logging


def add_logger(name, logging, level, console):
    handler = [x for x in logging.get('handlers') if name in x]
    if console:
        handler.extend(['console'])
    if logging.get('loggers'):
        logging.get('loggers').update({name: {
            'handlers': handler,
            "level": level,
            "propagate": "False"
        }})
    else:
        logging.update({'loggers': {name: {
            'handlers': handler,
            "level": level,
            "propagate": "False"
        }}})
    return logging


def add_logfile(name):
    debug = datetime.datetime.now().strftime(
        "%Y-%m-%d") + "_" + name + "_debug.log"
    error = datetime.datetime.now().strftime(
        "%Y-%m-%d") + "_" + name + "_error.log"
    info = datetime.datetime.now().strftime("%Y-%m-%d") + "_" + name + "_info.log"
    ret = {'debug': debug, 'error': error, 'info': info}
    return ret


def log_init(name, level, console, path, folder):
    """
    DEBUG > INFO > WARN > ERROR
    :return:
    """
    # -------定义日志路径------------
    BASE_DIR = os.path.abspath('..') if path is None else os.path.abspath(path)
    LOG_DIR = os.path.join(BASE_DIR, folder)
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)  # 创建路径

    # -------定义日志文件------------
    filedict = add_logfile(name)

    # -------日志基础内容------------
    LOGGING = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            'standard': {
                'format': '%(asctime)s [Pid:%(process)d] [%(filename)s:%(lineno)d] [%(levelname)s]- %(message)s'
            }
        },

        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "DEBUG",
                "formatter": "standard",
                "stream": "ext://sys.stdout"
            }
        }
    }

    LOGGING = add_handler(name, LOGGING, LOG_DIR, filedict)
    LOGGING = add_logger(name, LOGGING, level, console)

    return LOGGING


def generate_logger(loggername='default', level='DEBUG', console=True, path=None, folder='log'):
    logging.config.dictConfig(log_init(loggername, level, console, path, folder))
    logger = logging.getLogger(loggername)
    return logger
