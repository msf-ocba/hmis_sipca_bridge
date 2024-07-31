import json
import logging
from datetime import date


def save_json_file(filename, json_content, ident=4):
    """
    Save JSON content to a file with the provided filename.
    Args:
        filename: The name of the file to save JSON content.
        json_content: A dictionary representing the JSON content to be saved.
        ident: The number of spaces to use for JSON indentation (default is 4).
    :return: None
    """
    with open(filename, 'w', encoding='utf8') as outfile:
        json.dump(json_content, outfile, indent=ident, sort_keys=True, ensure_ascii=False)


def get_logger(program_name):
    # Logging setup
    today = date.today().strftime("%Y-%m-%d")
    filename_log = today + "_" + program_name + ".log"

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    # create file handler which logs error messages
    fh = logging.FileHandler(filename_log, encoding='utf-8')
    fh.setLevel(logging.INFO)
    # create console handler which logs even debug messages
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)
    # add the handlers to logger
    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger