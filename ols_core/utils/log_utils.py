# -*- coding: utf-8 -*-

"""
输出日志
Author: OPPO 王凯-W9032067
"""

from logging import handlers, getLogger, INFO, Formatter, StreamHandler
from os import path, makedirs
from time import localtime, strftime

BASE_DIR = path.dirname(path.abspath(__file__))
LOG_SAVE_DIR = path.dirname(path.dirname(path.dirname(BASE_DIR)))
# LOG_SAVE_DIR = BASE_DIR
LOG_DIR_NAME = "log"

class LogOutput:
    def __init__(self, filename, count=30, when='midnight', fmt='%(asctime)s - %(levelname)s: %(message)s'):
        self.logger = getLogger(filename)
        self.logger.setLevel(INFO)  # 设置日志级别
        if not self.logger.handlers:
            # format_str = Formatter(fmt)
            format_str = Formatter('[%(asctime)s] | [%(levelname)s] %(filename)s\t | %(message)s')
            screen_out = StreamHandler()  # 往屏幕上输出
            screen_out.setFormatter(format_str)  # 设置屏幕上显示的格式
            file_out = handlers.TimedRotatingFileHandler(filename=filename, when=when, backupCount=count,
                                                         encoding='utf-8')
            file_out.setFormatter(format_str)  # 设置文件里写入的格式
            self.logger.addHandler(screen_out)  # 把对象加到logger里
            self.logger.addHandler(file_out)

class LogUtils:
    def __init__(self):
        self.log_path = None
        self.log = None

    def log_obj(self):
        if self.log_path is None:
            self.log_path = path.join(LOG_SAVE_DIR, LOG_DIR_NAME)
            if not path.exists(self.log_path):
                makedirs(self.log_path, exist_ok=True)
        if self.log is None:
            self.log = LogOutput(path.join(self.log_path, f"{strftime('%Y_%m_%d', localtime())}.log"))
        self.log.logger.info(f'日志保存路径为：{self.log_path}')
        return self.log.logger

if __name__ == '__main__':
    taskmgr_log = LogUtils()
    taskmgr_log.log_obj()
    taskmgr_log.log.logger.info(f"info")
    taskmgr_log.log.logger.error(f"error")
