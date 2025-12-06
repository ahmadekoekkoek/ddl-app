"""
Unit tests for Configuration Manager
"""

import unittest
import os
import json
import tempfile
from pathlib import Path
from scraper.config_manager import ConfigurationManager, ScraperConfig

class TestConfigurationManager(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.config_path = Path(self.temp_dir) / "config.json"
        self.manager = ConfigurationManager(str(self.config_path))

    def tearDown(self):
        import shutil
        try:
            shutil.rmtree(self.temp_dir)
        except:
            pass

    def test_default_config(self):
        """Test loading default configuration."""
        config = self.manager.load()
        self.assertIsInstance(config, ScraperConfig)
        self.assertEqual(config.timeout, 30)
        self.assertEqual(config.threads_per_process, 4)

    def test_load_from_file(self):
        """Test loading configuration from file."""
        data = {
            "timeout": 60,
            "threads_per_process": 8,
            "organization_name": "Test Org"
        }
        with open(self.config_path, 'w') as f:
            json.dump(data, f)

        config = self.manager.load()
        self.assertEqual(config.timeout, 60)
        self.assertEqual(config.threads_per_process, 8)
        self.assertEqual(config.organization_name, "Test Org")

    def test_env_var_override(self):
        """Test environment variable overrides."""
        os.environ["SCRAPER_TIMEOUT"] = "120"
        os.environ["OUTPUT_FOLDER"] = "env_output"

        try:
            config = self.manager.load()
            self.assertEqual(config.timeout, 120)
            self.assertEqual(config.output_folder, "env_output")
        finally:
            del os.environ["SCRAPER_TIMEOUT"]
            del os.environ["OUTPUT_FOLDER"]

    def test_cli_args_override(self):
        """Test CLI arguments override."""
        cli_args = {
            "timeout": 90,
            "generate_pdf": False
        }
        config = self.manager.load(cli_args)
        self.assertEqual(config.timeout, 90)
        self.assertFalse(config.generate_pdf)

    def test_validation_error(self):
        """Test configuration validation."""
        # Invalid timeout
        with self.assertRaises(ValueError):
            self.manager.load({"timeout": -1})

    def test_save_config(self):
        """Test saving configuration."""
        config = self.manager.load({"organization_name": "Saved Org"})
        self.manager.save()

        # Verify file exists and content matches
        self.assertTrue(self.config_path.exists())
        with open(self.config_path, 'r') as f:
            saved_data = json.load(f)
        self.assertEqual(saved_data["organization_name"], "Saved Org")

        # Verify secrets are not saved
        self.assertEqual(saved_data.get("bearer_token", ""), "")

if __name__ == '__main__':
    unittest.main()
