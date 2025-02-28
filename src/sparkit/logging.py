import logging
import os


def create_default_logger(name: str | None = None):
    # Create logger
    logger = logging.getLogger(name=name)
    logger.setLevel(logging.INFO)
    name = logger.name

    # Create console handler and set level to INFO
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Create file handler for logging to file
    log_dir = "logs"  # Set your desired log directory
    os.makedirs(
        log_dir, exist_ok=True
    )  # Create the directory if it doesn't exist
    file_handler = logging.FileHandler(os.path.join(log_dir, f"{name}.log"))
    file_handler.setLevel(logging.INFO)

    # Create formatter and add it to the handlers
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
