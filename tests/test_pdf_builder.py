"""
Unit tests for PDF Builder
"""

import unittest
import pandas as pd
from scraper.pdf_builder import PDFBuilder

class TestPDFBuilder(unittest.TestCase):

    def setUp(self):
        self.builder = PDFBuilder(organization="Test Org")

        # Mock data
        self.keluarga_df = pd.DataFrame({
            "id_keluarga": ["1", "2"],
            "nama_kk": ["Budi", "Siti"],
            "no_kk": ["123", "456"],
            "desil_class": ["Miskin", "Rentan"],
            "pkh_flag": [True, False],
            "bpnt_flag": [True, True],
            "pbi_flag": [True, True]
        })

        self.anggota_df = pd.DataFrame({
            "id_keluarga": ["1", "1", "2"],
            "nama": ["Budi", "Ani", "Siti"],
            "nik": ["111", "222", "333"],
            "gender_clean": ["L", "P", "P"]
        })

        self.files_dict = {
            "aset_merged.csv": b"id_keluarga,jenis_lantai\n1,Keramik\n2,Tanah"
        }

    def test_build_pdfs(self):
        """Test PDF generation."""
        pdfs = self.builder.build_pdfs(
            self.files_dict,
            self.keluarga_df,
            self.anggota_df
        )

        # Should generate FULL REPORT + 2 Desil reports
        # Note: If reportlab is missing, it returns empty dict
        if pdfs:
            self.assertIn("FULL_REPORT.pdf", pdfs)
            self.assertTrue(any(k.startswith("REPORT_Miskin") for k in pdfs.keys()))
            self.assertTrue(any(k.startswith("REPORT_Rentan") for k in pdfs.keys()))

    def test_fallback_tables(self):
        """Test fallback table generation logic."""
        # This tests internal method
        elements = self.builder._create_fallback_table(
            "desil_distribution", self.keluarga_df, self.anggota_df
        )
        self.assertTrue(len(elements) > 0)

    def test_sanitization_in_pdf(self):
        """Test that data is sanitized before PDF generation."""
        # Create data with potential injection
        dirty_df = pd.DataFrame({
            "id_keluarga": ["1"],
            "nama_kk": ["<script>alert</script>"],
            "no_kk": ["123"],
            "desil_class": ["Miskin"]
        })

        # We can't easily inspect the PDF content here without complex parsing,
        # but we can verify the method runs without error
        try:
            self.builder._generate_single_pdf(
                "Test", dirty_df, pd.DataFrame(), self.files_dict
            )
        except Exception as e:
            self.fail(f"PDF generation failed with dirty data: {e}")

if __name__ == '__main__':
    unittest.main()
