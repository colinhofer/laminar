from logging import Formatter

BLUE = "\033[34m"
CYAN = "\033[36m"
YELLOW = "\033[33m"
GREEN = "\033[32m"
RED = "\033[31m"
MAGENTA = "\033[35m"
LIGHT_YELLOW = "\033[93m"
LIGHT_GREEN = "\033[92m"
GRAY = "\033[90m"
R = "\033[0m"

LEVEL_COLORS = {
    "DEBUG": CYAN,
    "INFO": GREEN,
    "WARNING": YELLOW,
    "ERROR": RED,
    "CRITICAL": MAGENTA,
}

STATUS_COLOR = {
    "RUNNING": f"{YELLOW} RUNNING {R}",
    "COMPLETED": f"{GREEN}COMPLETED{R}",
    "FAILED": f"{RED} FAILED  {R}",
    "UNKNOWN": f"{MAGENTA} UNKNOWN {R}",
    "PARTIAL": f"{MAGENTA} PARTIAL {R}",
}

TYPE_COLOR = {"MAIN": "MAIN", "TASK": "TASK", "HTTP": "HTTP", "DB": " DB "}

METHOD_COLOR = {
    "GET": "GET",
    "POST": f"{CYAN}POST{R}",
    "PATCH": f"{YELLOW}PATCH{R}",
    "PUT": f"{BLUE}PUT{R}",
    "DELETE": f"{RED}DELETE{R}",
    "OPTIONS": "OPTIONS",
}

class LogFormatter(Formatter):
    def format(self, record):
        level_color = LEVEL_COLORS.get(record.levelname, R)
        message = super().format(record)
        timestamp, level, msg = message.split(" | ", 2)
        colored_timestamp = f"{BLUE}{timestamp}{R}"
        colored_level = f"{level_color}{level}{R}"
        return f"{colored_timestamp} | {colored_level} | {msg}"
    
def format_log_data(data: dict):
    items = [
        TYPE_COLOR.get(data["log_type"]),
        STATUS_COLOR.get(data["status"]),
        f"{BLUE}{data.get('task')}{R}" if "task" in data else None,
        data.get("transaction"),
        data.get("table_name"),
        f"{data['rows']} rows" if "rows" in data else None,
        METHOD_COLOR.get(data.get("method")),
        str(data["status_code"]) if "status_code" in data else None,
        f"{GREEN}{data['duration']:.2f}s{R}" if "duration" in data else None,
        f"{GRAY}{data['url']}{R}" if "url" in data else None,
        f"{data['errors']} errors" if data.get("errors") else None,
        data.get("error_message") or data.get("message"),
    ]
    return " | ".join(filter(None, items))

