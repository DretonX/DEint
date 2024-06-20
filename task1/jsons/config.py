import logging
from logging.handlers import RotatingFileHandler

def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)  # Logging level INFO - All without DEBUG level


    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s') # Messages formater (time/logging level/ message)


    file_handler = RotatingFileHandler('/app/logs/main.log', maxBytes=10485760, backupCount=5) # Add rotation methot and max size of all log files
    file_handler.setFormatter(formatter)  # Run formatter


    logger.addHandler(file_handler) # handler to logger

    return logger
