# -*- coding: utf-8 -*-

"""
Enhance the default logger, print visual ascii effect for better readability.
"""

import typing as T
import sys
import enum
import logging
import contextlib
from functools import wraps
from datetime import datetime


def create_logger(
    name: T.Optional[str] = None,
    level: int = logging.INFO,
    log_format: str = "[User %(asctime)s] %(message)s",
    datetime_format: str = "%Y-%m-%d %H:%m:%S",
) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)

    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setLevel(level)
    formatter = logging.Formatter(
        fmt=log_format,
        datefmt=datetime_format,
    )
    stream_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)

    logger.parent = None  # disable handler from parent logger, only use stream handler

    return logger


DEFAULT_TAB = " " * 2


def encode_pipe(pipe: str) -> str:
    if len(pipe) == 1:
        return pipe + " "
    elif len(pipe) == 2 and pipe[1] == " ":
        return pipe
    else:  # pragma: no cover
        raise ValueError("the pipe symbol must be one character.")


DEFAULT_PIPE = encode_pipe("| ")


def format_line(
    msg: str,
    indent: int = 0,
    tab: str = DEFAULT_TAB,
    nest: int = 0,
    _pipes: T.Optional[T.List[str]] = None,
) -> str:
    """
    Format message with indentation and nesting.

    :param msg: the message to print.
    :param indent: the number of tab to indent.
    :param tab: the tab character.
    :param nest: the current nesting level. when nest = 0, it means there's no nesting.

    Example::

        >>> format_line("hello")
        '[User] | hello'
        >>> format_line("hello", indent=1)
        '[User] |   hello'
        >>> format_line("hello", nest=1)
        '[User] | | hello'
        >>> format_line("hello", indent=1, nest=1)
        '[User] | |   hello'

    Developer Note:

    - ``_pipes`` is a first in last out stack data structure that stores
        the list of pipe character for different level of nesting. When nest = 0,
        there should be only one pipe character in the list. When nest = 1, there
        should be two pipe characters in the list.
    """
    if _pipes is None:
        _pipes = [
            DEFAULT_PIPE,
        ] * (nest + 1)
    else:
        if len(_pipes) != (nest + 1):
            raise ValueError("the length of _pipes must be equal to nest + 1")
    nesting = "".join(_pipes)
    return f"{nesting}{tab * indent}{msg}"


class AlignEnum(str, enum.Enum):
    """
    Enum for aligning text in ruler. See :func:`format_ruler`.
    """
    left = "<"
    right = ">"
    middle = "^"


def format_ruler(
    msg: str,
    char: str = "-",
    align: AlignEnum = AlignEnum.middle,
    length: int = 80,
    left_padding: int = 5,
    right_padding: int = 5,
    corner: str = "",
    nest: int = 0,
    _pipes: T.Optional[T.List[str]] = None,
) -> str:
    """
    Format message to shape a horizontal ruler.

    :param msg: the message to print
    :param char: the character to use as ruler
    :param align: left, middle, right alignment of the message
    :param length: the total number of character of the ruler
    :param left_padding: the number of ruler character to pad on the left
    :param right_padding: the number of ruler character to pad on the right
    :param corner: the character to use as corner
    :param nest: the current nesting level. when nest = 0, it means there's no nesting.

    Example::

        >>> format_ruler("Hello", length=40)
        '---------------- Hello -----------------'

        >>> format_ruler("Hello", length=20)
        '------ Hello -------'

        >>> format_ruler("Hello", char="=", length=40)
        '================ Hello ================='

        >>> format_ruler("Hello", corner="+", length=40)
        '+--------------- Hello ----------------+'

        >>> format_ruler("Hello", align=AlignEnum.left, length=40)
        '----- Hello ----------------------------'

        >>> format_ruler("Hello", align=AlignEnum.right, length=40)
        '---------------------------- Hello -----'

        >>> format_ruler("Hello", left_padding=3, align=AlignEnum.left, length=40)
        '--- Hello ------------------------------'

        >>> format_ruler("Hello", right_padding=3, align=AlignEnum.right, length=40)
        '------------------------------ Hello ---'

        >>> format_ruler("Hello", right_padding=3, align=AlignEnum.right, length=40, nest=1)
        '| ------------------------------ Hello ---'

    Developer Note:

    - ``_pipes`` is a first in last out stack data structure that stores
        the list of pipe character for different level of nesting. When nest = 0,
        the ruler should not use any pipe character, so that ``_pipes``
        should be an empty list. When nest = 1, there should be one pipe characters
        in the list.
    """
    length = length - len(corner) * 2 - left_padding - right_padding - nest * 2
    msg = f" {msg} "
    left_pad = char * left_padding
    right_pad = char * right_padding
    if _pipes is None:
        _pipes = [
            DEFAULT_PIPE,
        ] * nest
    else:
        if len(_pipes) != nest:
            raise ValueError("the length of _pipes must be equal to nest")
    nesting = "".join(_pipes)
    s = f"{nesting}{corner}{left_pad}{msg:{char}{align.value}{length}}{right_pad}{corner}"
    return s


def decohints(decorator: T.Callable) -> T.Callable:
    """
    fix pycharm type hint bug for decorator.
    """
    return decorator


class VisLog:
    """
    A logger that supports nested logging.

    :param logger: any ``logging.Logger()`` object or any object what support
        ``logger.debug("message here")``, ``logger.info(...)``, ``logger.warning(...)``,
        ``logger.error(...)``, ``logger.critical(...)``. The visual logger will
        use this object to log the message. If not provided, a new logger will be created.
    :param name: a unique name for the logger.
    :param level: logging.INFO, logging.DEBUG, logging.WARNING, logging.ERROR, logging.CRITICAL.
    :param log_format: the format of the log message, see
        https://docs.python.org/3/library/logging.html#formatter-objects
        for more information.
    :param datetime_format: datetime format for the log message, default is ``%Y-%m-%d %H:%m:%S``
    :param tab: the indent string,
    :param pipe: the pipe character for nested log block, it has to be single character.
        default is "| ".
    """

    def __init__(
        self,
        logger: T.Optional[logging.Logger] = None,
        name: T.Optional[str] = None,
        level: int = logging.INFO,
        log_format: str = "[User %(asctime)s] %(message)s",
        datetime_format: str = "%Y-%m-%d %H:%m:%S",
        tab: str = DEFAULT_TAB,
        pipe: str = DEFAULT_PIPE,
    ):
        if logger is None:
            self._logger = create_logger(
                name=name,
                level=level,
                log_format=log_format,
                datetime_format=datetime_format,
            )
        else:  # pragma: no cover
            self._logger = logger

        # ``_indent`` stores the current level of indentation
        self._indent = 0
        self._tab = tab
        # ``_nest`` stores the current level of nesting
        self._nest = 0
        # ``_pipes`` is a first in last out stack data structure that stores
        # the list of pipe character for different level of nesting
        self._pipes = [
            pipe,
        ]

    def _pipe_start(
        self,
        pipe: T.Optional[str] = None,
    ) -> T.Optional[str]:
        if pipe is not None:
            pipe = encode_pipe(pipe)
            current_pipe = self._pipes.pop()
            self._pipes.append(pipe)
            return current_pipe
        else:
            return None

    def _pipe_end(
        self,
        pipe: T.Optional[str] = None,
        last_pipe: T.Optional[str] = None,
    ):
        if pipe is not None:
            self._pipes.pop()
            self._pipes.append(last_pipe)

    @contextlib.contextmanager
    def pipe(
        self,
        pipe: T.Optional[str] = None,
    ):
        """
        Temporarily change the pipe character for nested log block.

        Example:

        .. code-block:: python

            logger.info("a")
            with logger.pipe("*"):
                logger.info("b")
                logger.info("c")
            logger.info("d")

        The output looks like::

            [User] | a
            [User] * b
            [User] * c
            [User] | d
        """
        last_pipe = self._pipe_start(pipe)
        try:
            yield self
        finally:
            self._pipe_end(pipe, last_pipe)

    def _log(
        self,
        func: T.Callable,
        msg: str,
        indent: int = 0,
        tab: T.Optional[str] = None,
        pipe: T.Optional[str] = None,
    ) -> str:
        if tab is None:
            tab = self._tab
        with self.pipe(pipe=pipe):
            lines = msg.split("\n")
            for line in lines:
                output = format_line(
                    msg=line,
                    indent=self._indent + indent,
                    tab=tab,
                    nest=self._nest,
                    _pipes=self._pipes,
                )
                func(output)
        return output

    def debug(
        self,
        msg: str,
        indent: int = 0,
        tab: T.Optional[str] = None,
        pipe: T.Optional[str] = None,
    ) -> str:  # pragma: no cover
        """
        Todo: add docstring
        """
        return self._log(
            func=self._logger.debug,
            msg=msg,
            indent=indent,
            tab=tab,
            pipe=pipe,
        )

    def info(
        self,
        msg: str,
        indent: int = 0,
        tab: T.Optional[str] = None,
        pipe: T.Optional[str] = None,
    ) -> str:
        """
        Todo: add docstring
        """
        return self._log(
            func=self._logger.info,
            msg=msg,
            indent=indent,
            tab=tab,
            pipe=pipe,
        )

    def warning(
        self,
        msg: str,
        indent: int = 0,
        tab: T.Optional[str] = None,
        pipe: T.Optional[str] = None,
    ) -> str:  # pragma: no cover
        """
        Todo: add docstring
        """
        return self._log(
            func=self._logger.warning,
            msg=msg,
            indent=indent,
            tab=tab,
            pipe=pipe,
        )

    def error(
        self,
        msg: str,
        indent: int = 0,
        tab: T.Optional[str] = None,
        pipe: T.Optional[str] = None,
    ) -> str:  # pragma: no cover
        """
        Todo: add docstring
        """
        return self._log(
            func=self._logger.error,
            msg=msg,
            indent=indent,
            tab=tab,
            pipe=pipe,
        )

    def critical(
        self,
        msg: str,
        indent: int = 0,
        tab: T.Optional[str] = None,
        pipe: T.Optional[str] = None,
    ) -> str:  # pragma: no cover
        """
        Todo: add docstring
        """
        return self._log(
            func=self._logger.critical,
            msg=msg,
            indent=indent,
            tab=tab,
            pipe=pipe,
        )

    def ruler(
        self,
        msg: str,
        char: str = "-",
        align: AlignEnum = AlignEnum.left,
        length: int = 80,
        left_padding: int = 5,
        right_padding: int = 5,
        corner: str = "+",
        pipe: T.Optional[str] = None,
        func: T.Optional[T.Callable] = None,
    ) -> str:
        """
        Todo: add docstring
        """
        if func is None:
            func = self._logger.info

        with self.pipe(pipe=pipe):
            output = format_ruler(
                msg,
                char,
                align,
                length,
                left_padding,
                right_padding,
                corner,
                self._nest,
                self._pipes[:-1],
            )
            func(output)
        return output

    def _indent_start(self, level: int = 1):
        self._indent += level

    def _indent_end(self, level: int = 1):
        self._indent -= level

    @contextlib.contextmanager
    def indent(self, level: int = 1):
        """
        A context manager that temporarily increase the indentation level.

        Example:

        .. code-block:: python

            logger.ruler("start test indent")

            logger.info("a")

            with logger.indent():
                logger.info("b")

                with logger.indent():
                    logger.info("c")

                logger.info("d")

            logger.info("e")

            logger.ruler("end test indent")

        The output looks like::

            [User] +----- start test indent -----------------------------------+
            [User] | a
            [User] |   b
            [User] |     c
            [User] |   d
            [User] | e
            [User] +----- end test indent -------------------------------------+
        """
        self._indent_start(level=level)
        try:
            yield self
        finally:
            self._indent_end(level=level)

    def _nested_start(
        self,
        pipe: T.Optional[str] = None,
    ):
        self._nest += 1

        if pipe is None:
            self._pipes.append(DEFAULT_PIPE)
        else:  # pragma: no cover
            self._pipes.append(encode_pipe(pipe))

    def _nested_end(self):
        self._nest -= 1
        self._pipes.pop()

    @contextlib.contextmanager
    def nested(
        self,
        pipe: T.Optional[str] = None,
    ):
        """
        A context manager that nest logging for one more level.

        Example:

        .. code-block:: python

            logger.ruler("section 1")
            logger.info("hello 1")
            with logger.nested():
                logger.ruler("section 1.1")
                logger.info("hello 1.1")
                with logger.nested():
                    logger.ruler("section 1.1.1")
                    logger.info("hello 1.1.1")
                    logger.ruler("section 1.1.1")
                logger.ruler("section 1.1")
            logger.ruler("section 1")

        The output looks like::

            [User] +----- section 1 -------------------------------------------+
            [User] | hello 1
            [User] | +----- section 1.1 ---------------------------------------+
            [User] | | hello 1.1
            [User] | | +----- section 1.1.1 -----------------------------------+
            [User] | | | hello 1.1.1
            [User] | | +----- section 1.1.1 -----------------------------------+
            [User] | +----- section 1.1 ---------------------------------------+
            [User] +----- section 1 -------------------------------------------+
        """
        self._nested_start(pipe=pipe)
        try:
            yield self
        finally:
            self._nested_end()

    def pretty_log(
        self,
        start_msg: str = "Start {func_name}()",
        error_msg: str = "Error {func_name}(), elapsed = {elapsed:.2f} sec",
        end_msg: str = "End {func_name}(), elapsed = {elapsed:.2f} sec",
        char: str = "-",
        align: AlignEnum = AlignEnum.left,
        length: int = 80,
        left_padding: int = 5,
        right_padding: int = 5,
        corner: str = "+",
        nest: int = 0,
        pipe: T.Optional[str] = None,
    ):
        """
        A decorator that pretty print ruler when a function start, error, end.

        ``start_msg``, ``error_msg`` and ``end_msg`` are string template.
        the ``{func_name}`` will become the function you are decorating,
        the ``{elapsed}`` will become the execution time of the function.
        You can use ``{elapsed:.2f}`` to set the precision to two digits.
        The execution time measurement are not accuracy, it is just an estimation
        up to three digits precision.

        Example:

        .. code-block:: python

            @nested_logger.pretty_log(nest=1)
            def my_func2(name: str):
                time.sleep(1)
                nested_logger.info(f"{name} do something in my func 2")

            @nested_logger.pretty_log()
            def my_func1(name: str):
                time.sleep(1)
                nested_logger.info(f"{name} do something in my func 1")
                my_func2(name="bob")

            my_func1(name="alice")

        The output looks like::

            [User] +----- Start my_func1() ------------------------------------+
            [User] |
            [User] | alice do something in my func 1
            [User] | +----- Start my_func2() ----------------------------------+
            [User] | |
            [User] | | bob do something in my func 2
            [User] | |
            [User] | +----- End my_func2(), elapsed = 1.00 sec ----------------+
            [User] |
            [User] +----- End my_func1(), elapsed = 2.00 sec ------------------+

        :return: a decorator that you can put on top of your function
        """

        @decohints
        def deco(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                st = datetime.utcnow()

                for _ in range(nest):
                    self._nested_start(pipe=pipe)

                if nest == 0 and (pipe is not None):
                    last_pipe = self._pipe_start(pipe)

                self.ruler(
                    msg=start_msg.format(
                        func_name=func.__name__,
                        **kwargs,
                    ),
                    char=char,
                    align=align,
                    length=length,
                    left_padding=left_padding,
                    right_padding=right_padding,
                    corner=corner,
                )
                self.info("")

                try:
                    result = func(*args, **kwargs)
                except Exception as e:
                    et = datetime.utcnow()
                    elapsed = (et - st).total_seconds()
                    self.info("")
                    self.ruler(
                        msg=error_msg.format(
                            func_name=func.__name__,
                            elapsed=elapsed,
                            **kwargs,
                        ),
                        char=char,
                        align=align,
                        length=length,
                        left_padding=left_padding,
                        right_padding=right_padding,
                        corner=corner,
                    )
                    for _ in range(nest):
                        self._nested_end()

                    if nest == 0 and (pipe is not None):
                        self._pipe_end(pipe, last_pipe)
                    raise e

                et = datetime.utcnow()
                elapsed = (et - st).total_seconds()
                self.info("")
                self.ruler(
                    msg=end_msg.format(
                        func_name=func.__name__,
                        elapsed=elapsed,
                        **kwargs,
                    ),
                    char=char,
                    align=align,
                    length=length,
                    left_padding=left_padding,
                    right_padding=right_padding,
                    corner=corner,
                )

                for _ in range(nest):
                    self._nested_end()

                if nest == 0 and (pipe is not None):
                    self._pipe_end(pipe, last_pipe)

                return result

            return wrapper

        return deco

    def start_and_end(
        self,
        msg: str,
        start_emoji: str = "🟢",
        error_emoji: str = "🔴",
        end_emoji: str = "🟢",
        pipe: str = "| ",
    ):
        """
        A simplified version of the ``pretty_log`` decorator. Visually print
        the start and the end of a function.

        Example:

        .. code-block:: python

            @logger.start_and_end(
                msg="My Function 1",
                start_emoji="🟢",
                error_emoji="🔴",
                end_emoji="🟢",
                pipe="📦",
            )
            def my_func1(name: str):
                time.sleep(1)
                logger.info(f"{name} do something in my func 1")

            my_func1(name="alice")

        The output looks like::

            [User] +----- 🕑 🟢 Start 'My Function 1' --------------------------+
            [User] 📦
            [User] 📦 alice do something in my func 1
            [User] 📦
            [User] +----- ⏰ 🟢 End 'My Function 1', elapsed = 1.01 sec --------+

        :param msg: indicate the name of the function
        :param start_emoji: custom emoji for the start message
        :param end_emoji: custom emoji for the end message
        :param pipe: custom pipe character

        :return: a decorator that you can put on top of your function
        """
        if start_emoji and (not start_emoji.endswith(" ")):
            start_emoji = start_emoji + " "
        if error_emoji and (not error_emoji.endswith(" ")):
            error_emoji = error_emoji + " "
        if end_emoji and (not end_emoji.endswith(" ")):
            end_emoji = end_emoji + " "
        return self.pretty_log(
            start_msg=f"🕑 {start_emoji}Start {msg!r}",
            error_msg=f"⏰ {error_emoji}Error {msg!r}, elapsed = {{elapsed:.2f}} sec",
            end_msg=f"⏰ {end_emoji}End {msg!r}, elapsed = {{elapsed:.2f}} sec",
            pipe=pipe,
        )

    def emoji_block(
        self,
        msg: str,
        emoji: str,
    ):
        """
        A simplified version of the ``start_and_end`` decorator. Use emoji
        to Visually print the function logic block

        Example:

        .. code-block:: python

            @logger.emoji_block(
                msg="Deploy app {app_name}",
                emoji="🚀",
            )
            def deploy_app(app_name: str):
                logger.info("working ...")
                logger.info("done")

            deploy_app(app_name="my_app")

        The output looks like::

            [User] +----- 🕑 🚀 Start 'Deploy app my_app' ----------------------+
            [User] 🚀
            [User] 🚀 working ...
            [User] 🚀 done
            [User] 🚀
            [User] +----- ⏰ ✅ 🚀 End 'Deploy app my_app', elapsed = 1.01 sec -+

        :param msg: indicate the name of the function
        :param emoji: custom emoji for the visual effect

        :return: a decorator that you can put on top of your function
        """
        return self.start_and_end(
            msg=msg,
            start_emoji=emoji,
            error_emoji=f"❌ {emoji}",
            end_emoji=f"✅ {emoji}",
            pipe=emoji,
        )

    @contextlib.contextmanager
    def disabled(
        self,
        disable: bool = True,
    ):
        """
        Temporarily disable the logger. This is useful when you want to disable
        the logger without manually remove the ``logger.debug(...)`` code.
        For example, you can use logger in your unit test for debug, and then use
        this context manager to disable the logger when you run the test in CI.

        Example:

        .. code-block:: python

            # content of test.py

            def _test1():
                logger.info(...)

            def _test2():
                logger.info(...)

            def test_all():
                with logger.disabled(
                    disable=True, # this will disable all log
                    disable=False, # this will show log
                ):
                    _test1()
                    _test2()

        .. note::

            This method only works when your logger is automatically created by
            vislog, or it is a ``logging.Logger``.
        """
        try:
            if disable:
                existing_handlers = list(self._logger.handlers)
                self._logger.handlers.clear()
            yield self
        finally:
            if disable:
                for handler in existing_handlers:
                    self._logger.handlers.append(handler)
