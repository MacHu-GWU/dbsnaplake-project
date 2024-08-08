# -*- coding: utf-8 -*-


class DummyLogger:
    def debug(self, msg: str):
        pass

    def info(self, msg: str):
        pass

    def warning(self, msg: str):
        pass

    def error(self, msg: str):
        pass

    def critical(self, msg: str):
        pass


dummy_logger = DummyLogger()
