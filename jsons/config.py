import logging

def setup_logging():
    logging.basicConfig(
        filename='main.log',
        filemode='a',
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    return logging.getLogger()