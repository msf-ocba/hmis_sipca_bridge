import json
import logging
from datetime import date


def save_json_file(filename, json_content, ident=4):
    """
    Save JSON content to a file with the provided filename.

    Args:
        filename:     The name of the file to save JSON content.
        json_content: A dictionary representing the JSON content to be saved.
        ident:        The number of spaces to use for JSON indentation (default is 4).
    """
    with open(filename, "w", encoding="utf-8") as outfile:
        json.dump(json_content, outfile, indent=ident, sort_keys=True, ensure_ascii=False)


def get_logger(program_name):
    """
    Set up and return a logger that writes to both a timestamped log file
    and the console (stdout).

    Log file: log/YYYY-MM-DD_<program_name>.log
    File level  : INFO  (info, warning, error)
    Console level: DEBUG (all messages)
    """
    today = date.today().strftime("%Y-%m-%d")
    filename_log = f"log/{today}_{program_name}.log"

    logger = logging.getLogger(program_name)  # named logger avoids root logger conflicts
    logger.setLevel(logging.DEBUG)

    # Avoid adding duplicate handlers if get_logger is called more than once
    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # File handler — INFO and above
    fh = logging.FileHandler(filename_log, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)

    # Console handler — DEBUG and above
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger