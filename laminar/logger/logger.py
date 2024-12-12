import logging
import sys
from typing import TYPE_CHECKING

from .formatting import LogFormatter, format_log_data

if TYPE_CHECKING:
    from ..core.models import Log, Status

DEFAULT_LOG = "Laminar"

class Logger(logging.Logger):
    def __init__(self, name: str = None, level = 0):
        name = name or DEFAULT_LOG
        super().__init__(name, level)
        self.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        formatter = LogFormatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        self.addHandler(handler)

    def log_obj(self, obj: type["Log"]):
        data: dict = obj.model_dump(exclude_none=True)
        log_str = format_log_data(data)
        if data["status"] == "FAILED":
            self.error(log_str)
        else: 
            self.info(log_str)


