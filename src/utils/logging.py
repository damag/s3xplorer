import os
import logging
import logging.handlers
from datetime import datetime
from pathlib import Path

class Logger:
    """Centralized logging configuration for the application."""
    
    _instance = None
    
    @classmethod
    def get_instance(cls):
        """Get or create the singleton logger instance."""
        if cls._instance is None:
            cls._instance = Logger()
        return cls._instance
    
    def __init__(self):
        """Initialize the logger with configuration."""
        self.logger = logging.getLogger('s3xplorer')
        self.logger.setLevel(logging.INFO)
        
        # Create logs directory if it doesn't exist
        log_dir = Path.home() / '.s3xplorer' / 'logs'
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # File handler with daily rotation
        log_file = log_dir / f"s3xplorer_{datetime.now().strftime('%Y-%m-%d')}.log"
        file_handler = logging.handlers.RotatingFileHandler(
            filename=log_file,
            maxBytes=5*1024*1024,  # 5MB
            backupCount=10
        )
        file_handler.setLevel(logging.DEBUG)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Add handlers
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def set_level(self, level):
        """Set the logging level."""
        self.logger.setLevel(level)
    
    def get_logger(self):
        """Get the configured logger instance."""
        return self.logger

def get_logger():
    """Convenience function to get the configured logger."""
    return Logger.get_instance().get_logger() 