import logging
import os
from datetime import datetime


def setup_logging(log_dir="logs", level=logging.DEBUG):
    """Configure logging format."""

    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"{log_dir}/log_{timestamp}.log"

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(name)s.py/%(funcName)s() - %(message)s",
        filename=log_file
    )

    return logging.getLogger()
