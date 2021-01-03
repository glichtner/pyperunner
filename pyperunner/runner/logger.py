import os
import logging
import coloredlogs


def init_logger(
    log_path: str = None, log_format: str = None, log_level: int = logging.INFO
) -> logging.Logger:

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
            logging.FileHandler(os.path.join(log_path, "pipeline.log")), logging.DEBUG,
        )

    logging.captureWarnings(True)

    return logger


class StreamLogger:
    def __init__(self, logger: logging.Logger, level: int):
        self.logger = logger
        self.level = level
        self.linebuf = ""

    def write(self, buf: str) -> None:
        for line in buf.rstrip().splitlines():
            self.logger.log(self.level, line.rstrip())

    def flush(self) -> None:
        """
        Stub required for using this logger as a stream (stdout / stdterr).

        :return: None
        """
        pass
