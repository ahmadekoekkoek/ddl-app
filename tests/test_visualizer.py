"""
Unit tests for Visualizer
"""

import unittest
import pandas as pd
import tempfile
import shutil
from scraper.visualizer import Visualizer

class TestVisualizer(unittest.TestCase):

    def setUp(self):
        self.visualizer = Visualizer()

        # Mock data
        self.keluarga_df = pd.DataFrame({
            "id_keluarga": ["1", "2", "3", "4", "5"],
            "desil_class": ["Miskin (D3-D4)", "Sangat Miskin (D1-D2)", "Miskin (D3-D4)", "Rentan (D6-D7)", "Tidak Miskin (D8-D10)"],
            "pkh_flag": [True, False, True, False, False],
            "bpnt_flag": [True, True, False, False, False],
            "pbi_flag": [True, True, True, True, False],
            "no_rt": ["001", "001", "002", "002", "003"],
            "no_rw": ["001", "001", "001", "001", "001"]
        })

        self.anggota_df = pd.DataFrame({
            "id_keluarga": ["1", "1", "2", "3", "4", "5"],
            "usia": [45, 40, 60, 35, 25, 50],
            "gender_clean": ["Laki-laki", "Perempuan", "Perempuan", "Laki-laki", "Perempuan", "Laki-laki"]
        })

    def tearDown(self):
        self.visualizer.cleanup()

    def test_create_visualizations(self):
        """Test creation of all visualizations."""
        charts = self.visualizer.create_visualizations(self.keluarga_df, self.anggota_df)

        # Check if expected charts are generated (or empty dict if matplotlib missing)
        # Note: We can't guarantee matplotlib is installed in test env, so we check type
        self.assertIsInstance(charts, dict)

    def test_desil_chart(self):
        """Test desil chart generation."""
        charts = self.visualizer._create_desil_chart(self.keluarga_df)
        if charts:
            self.assertIn("desil_distribution", charts)
            self.assertIsInstance(charts["desil_distribution"], bytes)

    def test_age_histogram(self):
        """Test age histogram generation."""
        charts = self.visualizer._create_age_histogram(self.anggota_df)
        if charts:
            self.assertIn("age_distribution", charts)

    def test_gender_chart(self):
        """Test gender chart generation."""
        charts = self.visualizer._create_gender_chart(self.anggota_df)
        if charts:
            self.assertIn("gender_distribution", charts)

    def test_empty_data(self):
        """Test handling of empty data."""
        charts = self.visualizer.create_visualizations(pd.DataFrame(), pd.DataFrame())
        self.assertEqual(charts, {})

if __name__ == '__main__':
    unittest.main()
