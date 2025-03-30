"""Configuration management for the application."""

import os
import json
from pathlib import Path
import keyring
import logging
from typing import Dict, Any, Optional
from src.utils.logging import get_logger

logger = get_logger()

class Config:
    """Application configuration management."""
    
    _instance = None
    
    @classmethod
    def get_instance(cls):
        """Get or create the singleton config instance."""
        if cls._instance is None:
            cls._instance = Config()
        return cls._instance
    
    def __init__(self):
        """Initialize the configuration manager."""
        self.config_dir = Path.home() / '.s3xplorer'
        self.config_file = self.config_dir / 'config.json'
        
        # Default configuration
        self.defaults = {
            'theme': 'default',
            'default_region': 'us-east-1',
            'max_concurrent_operations': 5,
            'default_profile': '',
            'remember_credentials': True,
            'auto_refresh_interval': 0,  # 0 means disabled
            'show_hidden_objects': False,
            'confirm_deletions': True,
            'completed_operations_ttl': 5,  # Auto-remove completed operations after 5 seconds
            'operations_auto_cleanup': 1,  # Check for operations to clean up every 1 minute
            'auto_cleanup_enabled': True,  # Enable auto cleanup
            'auto_cleanup_age': 1  # Auto-cleanup operations older than 1 minute
        }
        
        # Ensure config directory exists
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
        # Load configuration
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file or create default if not exists."""
        try:
            if not self.config_file.exists():
                logger.info(f"Creating default configuration at {self.config_file}")
                with open(self.config_file, 'w') as f:
                    json.dump(self.defaults, f, indent=2)
                return self.defaults.copy()
            
            logger.info(f"Loading configuration from {self.config_file}")
            with open(self.config_file, 'r') as f:
                config = json.load(f)
                
            # Ensure all default keys exist
            for key, value in self.defaults.items():
                if key not in config:
                    config[key] = value
            
            return config
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            return self.defaults.copy()
    
    def save_config(self) -> bool:
        """Save current configuration to file."""
        try:
            logger.info(f"Saving configuration to {self.config_file}")
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Error saving configuration: {e}")
            return False
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value."""
        return self.config.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """Set a configuration value and save."""
        self.config[key] = value
        self.save_config()
    
    def get_credential(self, key: str) -> Optional[str]:
        """Get a credential from keyring."""
        try:
            return keyring.get_password("s3xplorer", key)
        except Exception as e:
            logger.error(f"Error retrieving credential {key}: {e}")
            return None
    
    def set_credential(self, key: str, value: str) -> bool:
        """Set a credential in keyring."""
        try:
            keyring.set_password("s3xplorer", key, value)
            return True
        except Exception as e:
            logger.error(f"Error setting credential {key}: {e}")
            return False
    
    def get_profiles(self) -> Dict[str, Dict[str, Any]]:
        """Get all saved connection profiles."""
        profile_file = self.config_dir / 'profiles.json'
        
        if not profile_file.exists():
            return {}
        
        try:
            with open(profile_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading profiles: {e}")
            return {}
    
    def save_profile(self, name: str, profile_data: Dict[str, Any]) -> bool:
        """Save a connection profile."""
        profile_file = self.config_dir / 'profiles.json'
        profiles = self.get_profiles()
        
        # Update or add profile
        profiles[name] = profile_data
        
        try:
            with open(profile_file, 'w') as f:
                json.dump(profiles, f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Error saving profile {name}: {e}")
            return False


def get_config() -> Config:
    """Get the configuration instance."""
    return Config.get_instance() 