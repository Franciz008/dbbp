import multiprocessing
import sys
import threading

from loguru import logger

log_file_path = './run_log.log'


class MyLogger:
    def __init__(self):
        self.logger = logger
        # 清空所有设置
        self.logger.remove()
        # 处理进程名
        t_name = threading.current_thread().name
        t_name = "Thread:{thread.name} | "
        p_name = multiprocessing.current_process().name
        #         p_name = "Thread:{process.name} | " if p_name.startswith(("T", "P")) else ''
        p_name = "Thread:{process.name} | "
        # 添加控制台输出的格式
        self.logger.add(sys.stdout,
                        format="<green>{time:YYYYMMDD HH:mm:ss}</green> | "
                               # f"{p_name}{t_name}"
                               "<cyan>{function}"
                               "</cyan>:<cyan>{line}</cyan> | "
                               "<level>{level}</level>: "
                               "<level>{message}</level>",
                        )
        # 输出到文件的格式,2022年4月20日09:18:44 关闭日志输出到文件
        self.logger.add(log_file_path, level='DEBUG',
                        format='{time:YYYYMMDD HH:mm:ss} - '
                               '{module}.{function}:{line}-{level} -{message}',
                        rotation="10 MB")

    def get_logger(self):
        return self.logger


clogger = MyLogger().get_logger()
