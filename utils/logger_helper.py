import logging

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

def configure_prefect_logging_to_file( logfile_name: str ):
    """
    Adds a FileHandler to all Prefect loggers so that flow and task logs
    are written both to the Prefect UI and to a local file.
    """
    file_handler = logging.FileHandler(logfile_name, mode="a", encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))

    # Attach to all Prefect loggers
    logger_names = [
        "prefect",
        "prefect.flow_runner",
        "prefect.task_runner",
        "prefect.engine",
    ]
    for name in logger_names:
        logger = logging.getLogger(name)
        if not any(isinstance(h, logging.FileHandler) and h.baseFilename == file_handler.baseFilename for h in logger.handlers):
            logger.addHandler(file_handler)