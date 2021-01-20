import os
import logging
from typing import Optional

import coloredlogs


def init_logger(
    log_path: Optional[str] = None,
    log_format: Optional[str] = None,
    log_level: int = logging.INFO,
) -> logging.Logger:
    """
    Setup logging instances

    Args:
        log_path: Path where log file will be saved - if None, no output to file
        log_format: log format to be used
        log_level: The minimal logging level that will be output

    Returns: Logger instance

    """

    logger = logging.getLogger()
    logger.setLevel(log_level)

    if log_format is None:
        log_format = (
            "%(asctime)s %(levelname)-8s %(processName)-12s %(name)-30s %(message)s"
        )

    coloredlogs.install(
        logger=logger,
        level=log_level,
        fmt=log_format,
        field_styles={
            "levelname": {"color": "blue"},
            "asctime": {"color": "green"},
            "processName": {"color": "cyan"},
        },
        level_styles={"debug": {}, "error": {"color": "red"}},
    )

    formatter = logging.Formatter(log_format, datefmt="%Y-%m-%d %H:%M:%S")

    def add_handler(handler: logging.Handler, level: int) -> None:
        handler.setLevel(level)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    if log_path is not None:
        add_handler(
            logging.FileHandler(os.path.join(log_path, "pipeline.log")), log_level,
        )

    logging.captureWarnings(True)

    return logger


class StreamLogger:
    def __init__(self, logger: logging.Logger, level: int):
        """
        Stream mock to reroute stdout and stderr to a logger instance

        Usage: Set sys.stdout and/or sys.stderr to instances of this class
        ``` python
        sys.stdout = StreamLogger(logger, logging.INFO)  # type: ignore
        sys.stderr = StreamLogger(logger, logging.ERROR)  # type: ignore
        ``` python

        Args:
            logger: Logger instance to which every call to write() will be logged to
            level: logging level when writing to the logger
        """
        self.logger = logger
        self.level = level
        self.linebuf = ""

    def write(self, buf: str) -> None:
        """
        Stream write function - writes to logger instance

        Args:
            buf: Text to write to logger

        Returns: None

        """
        for line in buf.rstrip().splitlines():
            self.logger.log(self.level, line.rstrip())

    def flush(self) -> None:
        """
        Stub required for using this logger as a stream (stdout / stderr).

        :return: None
        """
        pass
