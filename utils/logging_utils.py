import inspect
import logging
from pathlib import PurePath

from utils import Singleton


class UtilsLogger(Singleton):
    utils_loggers = {}
    level = logging.DEBUG
    ths_mod = ""
    ths_pkg = ""
    
    @classmethod
    def setup_logger(cls):
        log_key = (cls.ths_pkg, cls.ths_mod)
        if log_key in cls.utils_loggers:
            return cls.utils_loggers[log_key]
        
        logger_name = f"{cls.ths_pkg} => {cls.ths_mod}"
        # create logger
        new_logger = logging.getLogger(logger_name)
        new_logger.setLevel(cls.level)
        
        # create console handler and set level to inputted level
        ch = logging.StreamHandler()
        ch.setLevel(cls.level)
        
        # create formatter
        formatter = logging.Formatter(
            "%(levelname)s | %(name)s:%(message)s",
            datefmt="%H:%M:%S",
        )
        
        # add formatter to ch
        ch.setFormatter(formatter)
        
        # add ch to logger
        new_logger.addHandler(ch)
        
        cls.utils_loggers[log_key] = new_logger
        return cls.utils_loggers[log_key]
    
    @classmethod
    def __logger(cls, lvl, cl_path, msg, msg_args, fmt_args, kwargs):
        cls.ths_mod = cl_path.stem
        cls.ths_pkg = cl_path.parent.stem
        c_logger = cls.setup_logger()
        in_str = " ".join([str(xx) for xx in msg_args])
        msg = f"{msg} {in_str}"
        if fmt_args is None:
            fmt_args = tuple()
        getattr(c_logger, lvl)(msg, *fmt_args, **kwargs)
    
    @classmethod
    def debug(cls, msg, *args, fmt_args=None, **kwargs):
        pre_frame = inspect.currentframe().f_back
        pre_ln = pre_frame.f_lineno
        msg = f"{pre_ln} | {msg}"
        cl_path = PurePath(pre_frame.f_code.co_filename)
        cls.__logger("debug", cl_path, msg, args, fmt_args, kwargs)
    
    @classmethod
    def info(cls, msg, *args, fmt_args=None, **kwargs):
        pre_frame = inspect.currentframe().f_back
        pre_ln = pre_frame.f_lineno
        msg = f"{pre_ln} | {msg}"
        cl_path = PurePath(pre_frame.f_code.co_filename)
        cls.__logger("info", cl_path, msg, args, fmt_args, kwargs)
    
    @classmethod
    def warning(cls, msg, *args, fmt_args=None, **kwargs):
        pre_frame = inspect.currentframe().f_back
        pre_ln = pre_frame.f_lineno
        msg = f"{pre_ln} | {msg}"
        cl_path = PurePath(pre_frame.f_code.co_filename)
        cls.__logger("warning", cl_path, msg, args, fmt_args, kwargs)
    
    @classmethod
    def error(cls, msg, *args, fmt_args=None, **kwargs):
        pre_frame = inspect.currentframe().f_back
        pre_ln = pre_frame.f_lineno
        msg = f"{pre_ln} | {msg}"
        cl_path = PurePath(pre_frame.f_code.co_filename)
        cls.__logger("error", cl_path, msg, args, fmt_args, kwargs)
    
    @classmethod
    def critical(cls, msg, *args, fmt_args=None, **kwargs):
        pre_frame = inspect.currentframe().f_back
        pre_ln = pre_frame.f_lineno
        msg = f"{pre_ln} | {msg}"
        cl_path = PurePath(pre_frame.f_code.co_filename)
        cls.__logger("critical", cl_path, msg, args, fmt_args, kwargs)
    
    @classmethod
    def log(cls, msg, *args, fmt_args=None, **kwargs):
        pre_frame = inspect.currentframe().f_back
        pre_ln = pre_frame.f_lineno
        msg = f"{pre_ln} | {msg}"
        cl_path = PurePath(pre_frame.f_code.co_filename)
        cls.__logger("log", cl_path, msg, args, fmt_args, kwargs)
    
    @classmethod
    def exception(cls, msg, *args, fmt_args=None, **kwargs):
        pre_frame = inspect.currentframe().f_back
        pre_ln = pre_frame.f_lineno
        msg = f"{pre_ln} | {msg}"
        cl_path = PurePath(pre_frame.f_code.co_filename)
        cls.__logger("exception", cl_path, msg, args, fmt_args, kwargs)
