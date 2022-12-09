# Handler for logging to Loguru
import logging
import sys

class InterceptHandler(logging.Handler):
    def __init__(self, logger):
        super().__init__()
        self.logger = logger
    
    def emit(self, record):
        # Get corresponding Loguru level if it exists.
        try:
            level = self.logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message.
        frame, depth = sys._getframe(6), 6
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        self.logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())

# Intercept Discord logging
def intercept_discord_logging(logger):
    discord_logger = logging.getLogger('discord')
    discord_logger.setLevel(logging.DEBUG)
    discord_logger.addHandler(InterceptHandler(logger))