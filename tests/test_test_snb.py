"""
Tests for the test_snb modular DTSEN scraper package.
"""

import io
import sys
import os
import pytest
import pandas as pd

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from test_snb import (
    compute_age, map_desil, make_bansos_combo,
    safe_text, fmt_rupiah, fmt_date, pick_value,
    DataProcessor, Visualizer, ReportGenerator, PDFBuilder,
    DESIL_LABELS, COLS_TO_DROP
)


class TestUtils:
    """Test utility functions."""

    def test_compute_age_valid(self):
        assert compute_age("2000-01-01") is not None
        assert isinstance(compute_age("2000-01-01"), int)

    def test_compute_age_invalid(self):
        assert compute_age(None) is None
        assert compute_age("") is None
        assert compute_age("invalid") is None

    def test_map_desil(self):
        assert map_desil(1) == "DESIL_1"
        assert map_desil(5) == "DESIL_5"
        assert map_desil(6) == "DESIL_6_10"
        assert map_desil(10) == "DESIL_6_10"
        assert map_desil(None) == "DESIL_BELUM_DITENTUKAN"
        assert map_desil("") == "DESIL_BELUM_DITENTUKAN"

    def test_make_bansos_combo(self):
        assert make_bansos_combo(True, False, False) == "PKH"
        assert make_bansos_combo(True, True, False) == "PKH_BPNT"
        assert make_bansos_combo(True, True, True) == "PKH_BPNT_PBI"
        assert make_bansos_combo(False, False, False) == "NO_BANSOS"

    def test_safe_text(self):
        assert safe_text(None) == "-"
        assert safe_text("") == "-"
        assert safe_text("nan") == "-"
        assert safe_text("test") == "test"

    def test_fmt_rupiah(self):
        assert fmt_rupiah(1500000) == "Rp. 1.500.000"
        assert fmt_rupiah(None) == "-"
        assert fmt_rupiah("") == "-"

    def test_fmt_date(self):
        assert fmt_date("01/01/2000") == "01-01-2000"
        assert fmt_date(None) == "-"

    def test_pick_value(self):
        row = {"col1": "value1", "col2": "value2"}
        assert pick_value(row, ["col1", "col2"]) == "value1"
        assert pick_value(row, ["col3"]) == "-"


class TestDataProcessor:
    """Test DataProcessor class."""

    def test_clean_aset_empty(self):
        processor = DataProcessor()
        result = processor.clean_aset([])
        assert isinstance(result, pd.DataFrame)
        assert "id_keluarga" in result.columns

    def test_clean_aset_with_data(self):
        processor = DataProcessor()
        data = [{"id_keluarga": "123", "jenis_lantai": "keramik"}]
        result = processor.clean_aset(data)
        assert len(result) == 1
        assert "id_keluarga" in result.columns

    def test_clean_aset_bergerak_empty(self):
        processor = DataProcessor()
        result = processor.clean_aset_bergerak([])
        assert isinstance(result, pd.DataFrame)
        assert "id_keluarga" in result.columns

    def test_clean_aset_bergerak_with_data(self):
        processor = DataProcessor()
        data = [{"id_keluarga": "123", "jenis_aset": "sapi", "jumlah": 2}]
        result = processor.clean_aset_bergerak(data)
        assert "id_keluarga" in result.columns
        if not result.empty:
            assert "jml_sapi" in result.columns

    def test_build_keluarga_master_empty(self):
        processor = DataProcessor()
        result = processor.build_keluarga_master({})
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_build_keluarga_master_with_data(self):
        processor = DataProcessor()
        families_csv = pd.DataFrame({
            "id_keluarga": ["1", "2"],
            "no_kk": ["12345", "67890"],
            "desil_nasional": [1, 2]
        }).to_csv(index=False).encode()

        files_dict = {"families_raw.csv": families_csv}
        result = processor.build_keluarga_master(files_dict)

        assert len(result) == 2
        assert "bansos_combo" in result.columns
        assert "desil_class" in result.columns

    def test_build_anggota_master_empty(self):
        processor = DataProcessor()
        result = processor.build_anggota_master({})
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_build_anggota_master_with_data(self):
        processor = DataProcessor()
        members_csv = pd.DataFrame({
            "id_keluarga": ["1", "1", "2"],
            "nik": ["111", "112", "211"],
            "tgl_lahir": ["1990-01-01", "2010-05-15", "1985-12-25"]
        }).to_csv(index=False).encode()

        files_dict = {"members_raw.csv": members_csv}
        result = processor.build_anggota_master(files_dict)

        assert len(result) == 3
        assert "age" in result.columns

    def test_build_desil_sheets(self):
        processor = DataProcessor()
        keluarga = pd.DataFrame({
            "id_keluarga": ["1", "2", "3"],
            "desil_class": ["DESIL_1", "DESIL_2", "DESIL_1"]
        })
        result = processor.build_desil_sheets(keluarga)

        assert isinstance(result, dict)
        assert "DESIL_1" in result
        assert len(result["DESIL_1"]) == 2


class TestVisualizer:
    """Test Visualizer class."""

    def test_create_visualizations_empty(self):
        viz = Visualizer()
        result = viz.create_visualizations({})
        assert isinstance(result, dict)

    def test_create_visualizations_with_data(self):
        viz = Visualizer()
        families_csv = pd.DataFrame({
            "id_keluarga": ["1", "2"],
            "desil_nasional": [1, 2]
        }).to_csv(index=False).encode()

        members_csv = pd.DataFrame({
            "id_keluarga": ["1", "1", "2"],
            "tgl_lahir": ["1990-01-01", "2010-05-15", "1985-12-25"],
            "jenkel": ["1", "2", "1"]
        }).to_csv(index=False).encode()

        files_dict = {
            "families_raw.csv": families_csv,
            "members_raw.csv": members_csv,
        }

        keluarga = pd.DataFrame({
            "id_keluarga": ["1", "2"],
            "desil_class": ["DESIL_1", "DESIL_2"],
            "bansos_combo": ["PKH", "NO_BANSOS"]
        })

        members = pd.DataFrame({
            "id_keluarga": ["1", "1", "2"],
            "age": [30, 10, 35],
            "gender_clean": ["Laki-laki", "Perempuan", "Laki-laki"]
        })

        result = viz.create_visualizations(files_dict, keluarga, members)
        assert isinstance(result, dict)


class TestReportGenerator:
    """Test ReportGenerator class."""

    def test_build_xlsx_empty(self):
        gen = ReportGenerator()
        result = gen.build_xlsx({}, pd.DataFrame(), pd.DataFrame(), {})
        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_build_xlsx_with_data(self):
        gen = ReportGenerator()

        keluarga = pd.DataFrame({
            "id_keluarga": ["1", "2"],
            "no_kk": ["12345", "67890"],
            "desil_class": ["DESIL_1", "DESIL_2"],
            "bansos_combo": ["PKH", "NO_BANSOS"]
        })

        anggota = pd.DataFrame({
            "id_keluarga": ["1", "1", "2"],
            "nik": ["111", "112", "211"],
            "age": [30, 10, 35]
        })

        desil_sheets = {
            "DESIL_1": keluarga[keluarga["desil_class"] == "DESIL_1"],
            "DESIL_2": keluarga[keluarga["desil_class"] == "DESIL_2"],
        }

        result = gen.build_xlsx({}, keluarga, anggota, desil_sheets)
        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_build_xlsx_with_fallback(self):
        gen = ReportGenerator()
        result, fmt = gen.build_xlsx_with_fallback({}, pd.DataFrame(), pd.DataFrame(), {})
        assert fmt in ('xlsx', 'csv_zip')


class TestPDFBuilder:
    """Test PDFBuilder class."""

    def test_build_pdfs_empty(self):
        builder = PDFBuilder()
        result = builder.build_pdfs({}, pd.DataFrame(), pd.DataFrame())
        assert isinstance(result, dict)
        assert len(result) == 0

    def test_build_pdfs_with_data(self):
        builder = PDFBuilder()

        keluarga = pd.DataFrame({
            "id_keluarga": ["1"],
            "no_kk": ["12345"],
            "nama_kepala_keluarga": ["Test"],
            "desil_class": ["DESIL_1"]
        })

        anggota = pd.DataFrame({
            "id_keluarga": ["1"],
            "nik": ["111"],
            "nama": ["Member 1"]
        })

        result = builder.build_pdfs({}, keluarga, anggota)
        assert isinstance(result, dict)
        assert len(result) > 0
        assert "DESIL_1_REPORT.pdf" in result or "FULL_REPORT.pdf" in result


class TestConstants:
    """Test constants."""

    def test_desil_labels(self):
        assert len(DESIL_LABELS) == 7
        assert "DESIL_1" in DESIL_LABELS
        assert "DESIL_6_10" in DESIL_LABELS
        assert "DESIL_BELUM_DITENTUKAN" in DESIL_LABELS

    def test_cols_to_drop(self):
        assert "id_keluarga" in COLS_TO_DROP
        assert "idsemesta" in COLS_TO_DROP
        assert "pkh_flag" in COLS_TO_DROP
