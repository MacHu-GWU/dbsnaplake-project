# -*- coding: utf-8 -*-

"""
Provide the default logger that has no effect.
"""

class DummyLogger: # pragma: no cover
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
