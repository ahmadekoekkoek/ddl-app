"""
Configuration Manager
Robust configuration management with validation, environment variable support,
and secret handling.
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, field, asdict
from pathlib import Path
from enum import Enum

from core import get_logger

class Environment(Enum):
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"

@dataclass
class ScraperConfig:
    """Main configuration class."""

    # API Settings
    bearer_token: str = ""
    aes_key: str = ""
    base_url: str = "https://siks.kemensos.go.id"
    timeout: int = 30
    retry_limit: int = 3

    # Scraping Settings
    entity_lines: List[str] = field(default_factory=list)
    threads_per_process: int = 4
    chunk_size: int = 1000

    # Output Settings
    output_folder: str = "output"
    generate_excel: bool = True
    generate_pdf: bool = True
    generate_charts: bool = True

    # Feature Flags
    enable_caching: bool = True
    enable_circuit_breaker: bool = True

    # Branding
    logo_path: Optional[str] = None
    organization_name: str = "Kementerian Sosial"

    def __post_init__(self):
        """Validate configuration after initialization."""
        self._validate()

    def _validate(self):
        """Run validation rules."""
        errors = []

        if not self.bearer_token and os.getenv("BEARER_TOKEN"):
            self.bearer_token = os.getenv("BEARER_TOKEN")

        if not self.aes_key and os.getenv("AES_KEY"):
            self.aes_key = os.getenv("AES_KEY")

        # Basic validation
        if self.timeout < 1:
            errors.append("Timeout must be positive")
        if self.retry_limit < 0:
            errors.append("Retry limit must be non-negative")
        if self.threads_per_process < 1:
            errors.append("Threads per process must be at least 1")

        if errors:
            raise ValueError(f"Configuration errors: {'; '.join(errors)}")

class ConfigurationManager:
    """
    Manages loading and validation of configuration from multiple sources.
    Priority: CLI Args > Env Vars > Config File > Defaults
    """

    def __init__(self, config_path: str = "config.json"):
        self.config_path = Path(config_path)
        self._logger = get_logger('config_manager')
        self._config: Optional[ScraperConfig] = None

    def load(self, cli_args: Dict[str, Any] = None) -> ScraperConfig:
        """Load configuration from all sources."""
        # 1. Start with defaults
        config_data = {}

        # 2. Load from file
        if self.config_path.exists():
            try:
                with open(self.config_path, 'r') as f:
                    file_config = json.load(f)
                    config_data.update(file_config)
            except Exception as e:
                self._logger.warning(f"Failed to load config file: {e}")

        # 3. Load from environment variables
        env_map = {
            "BEARER_TOKEN": "bearer_token",
            "AES_KEY": "aes_key",
            "SCRAPER_TIMEOUT": "timeout",
            "SCRAPER_THREADS": "threads_per_process",
            "OUTPUT_FOLDER": "output_folder"
        }

        for env_var, config_key in env_map.items():
            val = os.getenv(env_var)
            if val:
                # Handle type conversion
                if config_key in ["timeout", "threads_per_process"]:
                    try:
                        config_data[config_key] = int(val)
                    except ValueError:
                        pass
                else:
                    config_data[config_key] = val

        # 4. Override with CLI args
        if cli_args:
            # Filter out None values
            clean_args = {k: v for k, v in cli_args.items() if v is not None}
            config_data.update(clean_args)

        # Create config object (runs validation)
        try:
            self._config = ScraperConfig(**{
                k: v for k, v in config_data.items()
                if k in ScraperConfig.__annotations__
            })
            self._logger.info("Configuration loaded successfully")
            return self._config
        except Exception as e:
            self._logger.error(f"Configuration validation failed: {e}")
            raise

    def get_config(self) -> ScraperConfig:
        """Get current configuration."""
        if not self._config:
            return self.load()
        return self._config

    def save(self, path: str = None):
        """Save current configuration to file."""
        save_path = path or self.config_path
        if not self._config:
            raise ValueError("No configuration to save")

        # Don't save secrets
        data = asdict(self._config)
        data['bearer_token'] = ""
        data['aes_key'] = ""

        with open(save_path, 'w') as f:
            json.dump(data, f, indent=2)

# Global instance
_config_manager = None

def get_config_manager() -> ConfigurationManager:
    global _config_manager
    if not _config_manager:
        _config_manager = ConfigurationManager()
    return _config_manager
