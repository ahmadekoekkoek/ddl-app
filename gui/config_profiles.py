"""
Configuration Profiles System
Save/load different scraping configurations with encryption for team sharing.
"""

import os
import json
import time
import base64
import hashlib
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from pathlib import Path

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from PySide6.QtCore import QObject, Signal


@dataclass
class ConfigProfile:
    """Represents a configuration profile."""
    name: str
    description: str = ""
    created_at: float = 0.0
    updated_at: float = 0.0
    environment: str = "production"  # production, staging, development

    # Scraping settings
    bearer_token: str = ""
    entity_lines: str = ""
    output_folder: str = ""

    # Performance settings
    concurrent_requests: int = 5
    request_timeout: int = 30
    retry_count: int = 3

    # Feature flags
    enable_caching: bool = True
    enable_logging: bool = True
    enable_metrics: bool = True

    # Metadata
    author: str = ""
    version: str = "1.0"
    tags: List[str] = None

    def __post_init__(self):
        if self.tags is None:
            self.tags = []
        if self.created_at == 0.0:
            self.created_at = time.time()
        self.updated_at = time.time()


class ProfileEncryption:
    """Handles encryption/decryption of profile data for secure sharing."""

    SALT_SIZE = 16

    @staticmethod
    def derive_key(password: str, salt: bytes) -> bytes:
        """Derive encryption key from password."""
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        return base64.urlsafe_b64encode(kdf.derive(password.encode()))

    @classmethod
    def encrypt(cls, data: str, password: str) -> bytes:
        """Encrypt data with password."""
        salt = os.urandom(cls.SALT_SIZE)
        key = cls.derive_key(password, salt)
        f = Fernet(key)
        encrypted = f.encrypt(data.encode())
        return salt + encrypted

    @classmethod
    def decrypt(cls, encrypted_data: bytes, password: str) -> str:
        """Decrypt data with password."""
        salt = encrypted_data[:cls.SALT_SIZE]
        encrypted = encrypted_data[cls.SALT_SIZE:]
        key = cls.derive_key(password, salt)
        f = Fernet(key)
        return f.decrypt(encrypted).decode()


class ProfileManager(QObject):
    """Manages configuration profiles."""

    profile_saved = Signal(str)  # profile name
    profile_loaded = Signal(str)  # profile name
    profile_deleted = Signal(str)  # profile name
    profiles_changed = Signal()

    PROFILES_DIR = "profiles"
    PROFILE_EXTENSION = ".profile"
    ENCRYPTED_EXTENSION = ".profile.enc"

    def __init__(self, base_path: str = None):
        super().__init__()
        self.base_path = Path(base_path or ".").resolve()
        self.profiles_path = self.base_path / self.PROFILES_DIR
        self.profiles_path.mkdir(exist_ok=True)

        self._current_profile: Optional[ConfigProfile] = None
        self._profiles_cache: Dict[str, ConfigProfile] = {}

    def save_profile(self, profile: ConfigProfile, encrypt: bool = False,
                     password: str = None) -> str:
        """Save a profile to disk."""
        profile.updated_at = time.time()

        # Sanitize name for filename
        safe_name = "".join(c for c in profile.name if c.isalnum() or c in "._- ")

        data = json.dumps(asdict(profile), indent=2)

        if encrypt and password:
            encrypted = ProfileEncryption.encrypt(data, password)
            file_path = self.profiles_path / f"{safe_name}{self.ENCRYPTED_EXTENSION}"
            with open(file_path, 'wb') as f:
                f.write(encrypted)
        else:
            file_path = self.profiles_path / f"{safe_name}{self.PROFILE_EXTENSION}"
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(data)

        self._profiles_cache[profile.name] = profile
        self.profile_saved.emit(profile.name)
        self.profiles_changed.emit()

        return str(file_path)

    def load_profile(self, name: str, password: str = None) -> Optional[ConfigProfile]:
        """Load a profile from disk."""
        safe_name = "".join(c for c in name if c.isalnum() or c in "._- ")

        # Try encrypted first
        enc_path = self.profiles_path / f"{safe_name}{self.ENCRYPTED_EXTENSION}"
        plain_path = self.profiles_path / f"{safe_name}{self.PROFILE_EXTENSION}"

        try:
            if enc_path.exists():
                if not password:
                    raise ValueError("Password required for encrypted profile")
                with open(enc_path, 'rb') as f:
                    encrypted = f.read()
                data = ProfileEncryption.decrypt(encrypted, password)
            elif plain_path.exists():
                with open(plain_path, 'r', encoding='utf-8') as f:
                    data = f.read()
            else:
                return None

            profile_dict = json.loads(data)
            profile = ConfigProfile(**profile_dict)

            self._current_profile = profile
            self._profiles_cache[name] = profile
            self.profile_loaded.emit(name)

            return profile

        except Exception as e:
            print(f"Error loading profile '{name}': {e}")
            return None

    def delete_profile(self, name: str) -> bool:
        """Delete a profile."""
        safe_name = "".join(c for c in name if c.isalnum() or c in "._- ")

        deleted = False
        for ext in [self.PROFILE_EXTENSION, self.ENCRYPTED_EXTENSION]:
            path = self.profiles_path / f"{safe_name}{ext}"
            if path.exists():
                path.unlink()
                deleted = True

        if deleted:
            if name in self._profiles_cache:
                del self._profiles_cache[name]
            self.profile_deleted.emit(name)
            self.profiles_changed.emit()

        return deleted

    def list_profiles(self) -> List[Dict[str, Any]]:
        """List all available profiles with metadata."""
        profiles = []

        for file in self.profiles_path.iterdir():
            if file.suffix not in [self.PROFILE_EXTENSION, '.enc']:
                continue

            is_encrypted = file.name.endswith(self.ENCRYPTED_EXTENSION)
            name = file.stem.replace('.profile', '')

            info = {
                "name": name,
                "path": str(file),
                "encrypted": is_encrypted,
                "size": file.stat().st_size,
                "modified": file.stat().st_mtime,
            }

            # Try to read metadata from unencrypted profiles
            if not is_encrypted:
                try:
                    with open(file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    info.update({
                        "description": data.get("description", ""),
                        "environment": data.get("environment", ""),
                        "author": data.get("author", ""),
                        "version": data.get("version", ""),
                        "tags": data.get("tags", []),
                    })
                except Exception:
                    pass

            profiles.append(info)

        return sorted(profiles, key=lambda x: x["modified"], reverse=True)

    def get_current_profile(self) -> Optional[ConfigProfile]:
        """Get currently loaded profile."""
        return self._current_profile

    def create_from_config(self, name: str, config_path: str = "config.json") -> Optional[ConfigProfile]:
        """Create a profile from existing config.json."""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)

            profile = ConfigProfile(
                name=name,
                bearer_token=config.get("bearer_token", ""),
                entity_lines=config.get("entity_lines", ""),
                output_folder=config.get("output_folder", ""),
            )

            return profile

        except Exception as e:
            print(f"Error creating profile from config: {e}")
            return None

    def apply_profile(self, profile: ConfigProfile, config_path: str = "config.json"):
        """Apply a profile to config.json."""
        config = {
            "bearer_token": profile.bearer_token,
            "entity_lines": profile.entity_lines,
            "output_folder": profile.output_folder,
        }

        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2)

        self._current_profile = profile

    def export_for_sharing(self, profile: ConfigProfile, password: str,
                          exclude_sensitive: bool = True) -> bytes:
        """Export a profile for team sharing."""
        profile_copy = ConfigProfile(**asdict(profile))

        if exclude_sensitive:
            # Mask sensitive data
            profile_copy.bearer_token = ""

        data = json.dumps(asdict(profile_copy), indent=2)
        return ProfileEncryption.encrypt(data, password)

    def import_shared(self, encrypted_data: bytes, password: str) -> Optional[ConfigProfile]:
        """Import a shared profile."""
        try:
            data = ProfileEncryption.decrypt(encrypted_data, password)
            profile_dict = json.loads(data)
            return ConfigProfile(**profile_dict)
        except Exception as e:
            print(f"Error importing shared profile: {e}")
            return None


class EnvironmentManager:
    """Manages environment-specific configurations."""

    ENVIRONMENTS = ["development", "staging", "production"]

    def __init__(self, profile_manager: ProfileManager):
        self.profile_manager = profile_manager
        self._env_configs: Dict[str, ConfigProfile] = {}

    def set_environment(self, env: str, profile: ConfigProfile):
        """Set configuration for an environment."""
        if env not in self.ENVIRONMENTS:
            raise ValueError(f"Invalid environment: {env}")
        profile.environment = env
        self._env_configs[env] = profile

    def get_environment_config(self, env: str) -> Optional[ConfigProfile]:
        """Get configuration for an environment."""
        return self._env_configs.get(env)

    def switch_environment(self, env: str, config_path: str = "config.json") -> bool:
        """Switch to a different environment."""
        if env not in self._env_configs:
            return False

        profile = self._env_configs[env]
        self.profile_manager.apply_profile(profile, config_path)
        return True

    def get_current_environment(self) -> Optional[str]:
        """Get current environment name."""
        current = self.profile_manager.get_current_profile()
        if current:
            return current.environment
        return None


# Singleton instance
_profile_manager: Optional[ProfileManager] = None

def get_profile_manager(base_path: str = None) -> ProfileManager:
    """Get the global profile manager instance."""
    global _profile_manager
    if _profile_manager is None:
        _profile_manager = ProfileManager(base_path)
    return _profile_manager
