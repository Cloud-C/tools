import logging
import os
import sys
import traceback
from logging.handlers import TimedRotatingFileHandler
from config import Config


# logged messages to help debug
class DebugTool:
    """
        *** error and critical method can logged traceback ***

        How to use:
            DebugTool.<method>(exception=<Exception object>, msg=<message string>)

        Example:

            try:
                ...

            except IOError as e:

                DebugTool.debug(e)
                DebugTool.debug(msg='occur IOError exception')
                DebugTool.debug(e, msg='occur IOError exception')

        * title: 設定日誌檔名開頭名稱
        * do_print: 是否print在terminal上
        * log_methods: 設定欲新建的日誌名稱和屬性
    """
    title = getattr(Config, 'SYSTEM_NAME', 'default_name')
    do_print = True
    log_methods = {
        'debug': ['/log/debug_log/', logging.DEBUG],
        'error': ['/log/error_log/', logging.ERROR],
        'critical': ['/log/critical_log/', logging.CRITICAL]
    }
    abs_path = os.path.abspath('.')

    @classmethod
    def _init_file_path(cls):
        for key, value in cls.log_methods.items():
            path = value[0]
            os.makedirs(f'{cls.abs_path}{path}', exist_ok=True)

    @staticmethod
    def _get_file_handler(file_name, level=logging.DEBUG):
        """
            log存成檔案
        """
        console = TimedRotatingFileHandler(
            file_name, when='H', interval=1, backupCount=10000, encoding=None, delay=False, utc=False)
        console.setLevel(level)
        formatter = logging.Formatter('[%(asctime)-s] [%(levelname)-8s] [%(message)s]')
        console.setFormatter(formatter)
        return console

    @staticmethod
    def _get_stream_handler(level=logging.DEBUG):
        """
            print到終端上的
        """
        console = logging.StreamHandler(sys.stdout)
        console.setLevel(level)
        formatter = logging.Formatter('[%(asctime)s] [%(levelname)-8s] [%(message)s]')
        console.setFormatter(formatter)
        return console

    @staticmethod
    def _set_loggers(console_list):
        console_list = console_list if console_list else []
        for console in console_list:
            logging.getLogger().addHandler(console)

    @staticmethod
    def _remove_info():
        logging.getLogger('requests').setLevel(logging.WARNING)
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('apscheduler').setLevel(logging.WARNING)
        logging.getLogger('socketio').setLevel(logging.WARNING)
        logging.getLogger('engineio').setLevel(logging.WARNING)
        logging.getLogger('PIL').setLevel(logging.WARNING)
        logging.captureWarnings(True)

    @staticmethod
    def _get_logger():
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        return logger

    @staticmethod
    def _have_traceback(method, exception, msg):
        traceback_result = traceback.format_exc().strip('\n')
        if exception and msg:
            method(f'{msg}] [{str(exception)}]\n[{traceback_result}')
        elif exception:
            method(f'{str(exception)}]\n[{traceback_result}')
        elif msg:
            method(f'{msg}]\n[{traceback_result}')

    @staticmethod
    def _no_traceback(method, exception, msg):
        if exception and msg:
            method(f'{msg}] [{str(exception)}')
        elif exception:
            method(exception)
        elif msg:
            method(msg)

    @classmethod
    def start_logging(cls):
        cls._init_file_path()
        console_list = []
        if cls.do_print:
            stream_handler = cls._get_stream_handler()
            console_list.append(stream_handler)
        for key, value in cls.log_methods.items():
            file_handler = cls._get_file_handler(
                file_name=f'{cls.abs_path}{value[0]}{cls.title}.{key}.log', level=value[1])
            console_list.append(file_handler)
        cls._set_loggers(console_list=console_list)
        cls._remove_info()

    @classmethod
    def debug(cls, exception=None, msg=None):
        logger = cls._get_logger()
        cls._no_traceback(method=logger.debug, exception=exception, msg=msg)

    @classmethod
    def info(cls, exception=None, msg=None):
        logger = cls._get_logger()
        cls._no_traceback(method=logger.info, exception=exception, msg=msg)

    @classmethod
    def warning(cls, exception=None, msg=None):
        logger = cls._get_logger()
        cls._no_traceback(method=logger.warning, exception=exception, msg=msg)

    @classmethod
    def error(cls, exception=None, msg=None):
        logger = cls._get_logger()
        cls._have_traceback(method=logger.error, exception=exception, msg=msg)

    @classmethod
    def critical(cls, exception=None, msg=None):
        logger = cls._get_logger()
        cls._have_traceback(method=logger.critical, exception=exception, msg=msg)


DebugTool.start_logging()
