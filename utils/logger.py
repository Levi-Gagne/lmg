import logging
import sys
import re
from typing import Optional
from pathlib import Path

from utils.colorConfig import Color as C

CUSTOM_LOG_LEVELS = {
    "VALIDATE": 25,
    "WRITE": 24,
    "APPEND": 23,
    "DELETE": 22,
    "OVERWRITE": 21,
    "SANITIZE": 15,
    "SUMMARY": 16,
    "SUCCESS": 14,
}

for name, level in CUSTOM_LOG_LEVELS.items():
    logging.addLevelName(level, name)

def _make_custom_logger_method(level, category):
    def log_for_level(self, msg, *args, **kwargs):
        # No prefix, let the formatter add [LEVEL]
        if self.isEnabledFor(level):
            self._log(level, msg, args, **kwargs)
    return log_for_level

for name, level in CUSTOM_LOG_LEVELS.items():
    setattr(logging.Logger, name.lower(), _make_custom_logger_method(level, name.capitalize()))

THEME_COLORS = {
    "normal": {
        "DEBUG":     C.b + C.cyan,
        "INFO":      C.green,
        "WARNING":   C.b + C.yellow,
        "ERROR":     C.rv + C.red,
        "CRITICAL":  C.b + C.rv + C.purple,
        "VALIDATE":  C.b + C.u + C.blue,
        "WRITE":     C.b + C.lime_green,
        "APPEND":    C.b + C.lime_green,
        "DELETE":    C.st + C.red,
        "OVERWRITE": C.b + C.rv + C.soft_orange,
        "SANITIZE":  C.u + C.cyan,
        "SUMMARY":   C.b + C.ivory,
        "SUCCESS":   C.b + C.lime_green,
        "RESET":     C.r
    }
}

def strip_ansi(text: str) -> str:
    ansi_escape = re.compile(r'\x1B[@-_][0-?]*[ -/]*[@-~]')
    return ansi_escape.sub('', text)

class SmartFormatter(logging.Formatter):
    def __init__(self, theme: str = "normal", color: bool = True):
        super().__init__(fmt="%(asctime)s %(message)s", datefmt="%H:%M:%S")
        self.theme = theme
        self.color = color

    def format(self, record: logging.LogRecord) -> str:
        msg = record.getMessage()
        theme = THEME_COLORS.get(self.theme, THEME_COLORS["normal"])
        levelname = record.levelname.upper()

        if self.color and levelname in theme:
            color_style = theme[levelname]
            reset = theme["RESET"]
            msg = f"{color_style}[{levelname}]{reset} {msg}"
        else:
            msg = f"[{levelname}] {msg}"

        timestamp = self.formatTime(record, datefmt="%H:%M:%S")
        if self.color:
            return f"{timestamp} {msg}"
        else:
            return f"{timestamp} {strip_ansi(msg)}"

def install_smart_logger(
    theme: str = "normal",
    level: int = logging.INFO,
    log_to_notebook: bool = True,
    log_to_file: bool = False,
    log_file: Optional[str] = None,
    color: bool = True
) -> logging.Logger:
    logger = logging.getLogger()
    logger.handlers.clear()

    # Notebook/console handler
    if log_to_notebook:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(level)
        stream_handler.setFormatter(SmartFormatter(theme=theme, color=color))
        logger.addHandler(stream_handler)

    # File handler (plain, no color)
    if log_to_file:
        if not log_file:
            raise ValueError("If log_to_file=True, must provide log_file path.")
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        class PlainFormatter(logging.Formatter):
            def format(self, record: logging.LogRecord) -> str:
                s = SmartFormatter(theme=theme, color=False).format(record)
                return strip_ansi(s)
        file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(PlainFormatter())
        logger.addHandler(file_handler)

    logger.setLevel(level)
    return logger