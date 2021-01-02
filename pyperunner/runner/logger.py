import os
import logging
import coloredlogs


def init_logger(log_path=None, log_format=None, log_level=logging.INFO):

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

    def add_handler(handler, name, level):
        handler.set_name(name)
        handler.setLevel(level)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    if log_path is not None:
        add_handler(
            logging.FileHandler(os.path.join(log_path, "pipeline.log")),
            "log_file",
            logging.DEBUG,
        )

    logging.captureWarnings(True)

    return logger
