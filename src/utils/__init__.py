"""
Utility modules for the application.
"""

from src.utils.logging import get_logger
from src.utils.config import get_config
from src.utils.file_service import get_file_service
from src.utils.theme import apply_theme, get_theme_manager
import logging
from logging.handlers import RotatingFileHandler

__all__ = ['get_logger', 'get_config', 'get_file_service', 'apply_theme', 'get_theme_manager']

def get_logger():
    """Get the application logger."""
    logger = logging.getLogger('s3xplorer')
    if not logger.handlers:
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        
        # Set debug level to see progress updates
        logger.setLevel(logging.DEBUG)
        console_handler.setLevel(logging.DEBUG)
        
        logger.addHandler(console_handler)
        
        # File handler for persistent logging
        logs_dir = get_config_dir() / 'logs'
        logs_dir.mkdir(exist_ok=True)
        
        log_file = logs_dir / 'app.log'
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)
        
    return logger 