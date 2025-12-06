"""
Scraper Constants
API endpoints, configuration, and mappings.
"""

# API Endpoints
URL_FAMILY = "https://api.kemensos.go.id/dtsen/view-dtsen/v1/get-keluarga-dtsen"
URL_MEMBERS = "https://api.kemensos.go.id/dtsen/view-dtsen/v1/get-anggota-keluarga-dtsen-by-id-keluarga"
URL_KYC = "https://api.kemensos.go.id/dtsen/view-dtsen/v1/get-daftar-kyc-keluarga-dtsen"
URL_PBI = "https://api.kemensos.go.id/dtsen/bansos/v1/get-riwayat-bansos-pbi-by-id-keluarga"
URL_PKH = "https://api.kemensos.go.id/dtsen/bansos/v1/get-riwayat-bansos-by-id-keluarga"
URL_BPNT = "https://api.kemensos.go.id/dtsen/bansos/v1/get-riwayat-bansos-bpnt-by-id-keluarga"
URL_ASET = "https://api.kemensos.go.id/dtsen/aset/v1/get-aset-keluarga-by-id-keluarga"
URL_ASET_BERGERAK = "https://api.kemensos.go.id/dtsen/aset/v1/get-aset-keluarga-bergerak-by-id-keluarga"

# Endpoint mapping for batch fetching
ENDPOINTS = {
    "members": URL_MEMBERS,
    "kyc": URL_KYC,
    "pbi": URL_PBI,
    "pkh": URL_PKH,
    "bpnt": URL_BPNT,
    "aset": URL_ASET,
    "asetb": URL_ASET_BERGERAK,
}

# Scraper configuration
THREADS_PER_PROCESS = 20
SLEEP_BETWEEN_REQUESTS = 0.15
RETRY_LIMIT = 3
TIMEOUT = 40

# Per-endpoint tuning
ENDPOINT_TUNING = {
    "PKH": {"workers": 16, "sleep_range": (0.05, 0.15)},
    "BPNT": {"workers": 16, "sleep_range": (0.05, 0.15)},
    "PBI": {"workers": 16, "sleep_range": (0.06, 0.18)},
    "DEFAULT": {"workers": THREADS_PER_PROCESS, "sleep_range": (SLEEP_BETWEEN_REQUESTS, SLEEP_BETWEEN_REQUESTS + 0.1)},
}

# PDF page size (F4)
from reportlab.lib.units import cm
PAGE_SIZE_F4 = (21.5 * cm, 33.0 * cm)

# Data mappings for reports
FAMILY_HEADERS = [
    ("No KK", ["no_kk", "NO_KK"]),
    ("Nama Kepala Keluarga", ["nama_kepala_keluarga", "NAMA_KEPALA_KELUARGA", "nama_kk"]),
    ("Jumlah Anggota Keluarga", ["jumlah_anggota_calc", "jumlah_anggota", "jml_anggota", "jumlah_art"]),
    ("Alamat", ["alamat", "ALAMAT", "alamat_lengkap"]),
    ("RT", ["no_rt", "rt", "RT"]),
    ("RW", ["no_rw", "rw", "RW"]),
    ("Desil Nasional", ["desil_nasional", "desil", "DESIL"]),
    ("Peringkat Nasional", ["peringkat_nasional", "peringkat"]),
]

MEMBER_HEADERS = [
    ("Nama", ["nama", "nama_lengkap", "NAMA_LENGKAP", "nama_anggota"]),
    ("NIK", ["nik", "NIK", "nik_anggota"]),
    ("Tgl Lahir", ["tgl_lahir", "tanggal_lahir", "TGL_LAHIR"]),
    ("Jenis Kelamin", ["gender_clean", "jenis_kelamin", "jenkel", "id_jenis_kelamin"]),
    ("Hubungan Keluarga", ["hub_kepala_keluarga", "hubungan_keluarga", "hubungan", "status_hubungan"]),
    ("Status Kawin", ["sts_kawin", "status_kawin"]),
]

ASSET_IMMOVABLE = [
    ("Status Penguasaan Bangunan", ["status_penguasaan_bangunan", "status_lahan", "kepemilikan_rumah"]),
    ("Lantai Terluas", ["jenis_lantai", "lantai_terluas", "lantai"]),
    ("Dinding Terluas", ["jenis_dinding", "dinding_terluas", "dinding"]),
    ("Atap Terluas", ["jenis_atap", "atap_terluas", "atap"]),
    ("Sumber Air Minum", ["sumber_air_minum", "air_minum"]),
    ("Jarak Sumber Air Limbah", ["jarak_sumber_air_limbah", "jarak_tinja", "jarak_pembuangan"]),
    ("Sumber Penerangan", ["sumber_penerangan", "penerangan"]),
    ("Bahan Bakar Utama", ["bahan_bakar_utama", "bahan_bakar_memasak", "bahan_bakar"]),
    ("Fasilitas BAB", ["fasilitas_bab", "kepemilikan_kamar_mandi"]),
    ("Jenis Kloset", ["jenis_kloset", "kloset"]),
    ("Pembuangan Tinja", ["pembuangan_tinja", "tempat_pembuangan_akhir_tinja"]),
]

ASSET_MOVABLE = [
    ("Jumlah Sapi", ["jml_sapi", "jumlah_sapi", "sapi"]),
    ("Jumlah Kerbau", ["jml_kerbau", "jumlah_kerbau", "kerbau"]),
    ("Jumlah Kambing/Domba", ["jml_kambing_domba", "jumlah_kambing", "jumlah_domba", "kambing", "domba"]),
    ("Jumlah Babi", ["jml_babi", "jumlah_babi", "babi"]),
    ("Jumlah Kuda", ["jml_kuda", "jumlah_kuda", "kuda"]),
    ("Air Conditioner (AC)", ["ac", "air_conditioner"]),
    ("Emas/Perhiasan min 10 gr", ["emas", "perhiasan"]),
    ("Kapal/Perahu Motor", ["kapal_perahu_motor", "kapal", "perahu_motor"]),
    ("Komputer/Laptop/Tablet", ["komputer", "laptop", "tablet"]),
    ("Lemari Es/Kulkas", ["kulkas", "lemari_es"]),
    ("Mobil", ["mobil"]),
    ("Pemanas Air (Water Heater)", ["pemanas_air", "water_heater"]),
    ("Perahu", ["perahu"]),
    ("Sepeda", ["sepeda"]),
    ("Sepeda Motor", ["sepeda_motor", "motor"]),
    ("Smartphone", ["smartphone", "hp"]),
    ("Tabung Gas 5.5 kg atau lebih", ["tabung_gas"]),
    ("Telepon Rumah (PSTN)", ["telepon_rumah", "telepon"]),
    ("Televisi Layar Datar min 30 inch", ["televisi", "tv_flat", "tv"]),
]

# Asset normalization aliases (use jml_ prefix for legacy compatibility)
ASSET_ALIASES = {
    # Livestock
    "sapi": "jml_sapi",
    "jumlahsapi": "jml_sapi",
    "jmlsapi": "jml_sapi",
    "kerbau": "jml_kerbau",
    "jumlahkerbau": "jml_kerbau",
    "jmlkerbau": "jml_kerbau",
    "kambing": "jml_kambing_domba",
    "domba": "jml_kambing_domba",
    "kambingdomba": "jml_kambing_domba",
    "jumlahkambingdomba": "jml_kambing_domba",
    "jmlkambing": "jml_kambing_domba",
    "jmlkambingdomba": "jml_kambing_domba",
    "babi": "jml_babi",
    "jumlahbabi": "jml_babi",
    "jmlbabi": "jml_babi",
    "kuda": "jml_kuda",
    "jumlahkuda": "jml_kuda",
    "jmlkuda": "jml_kuda",
    # Electronics & Appliances
    "ac": "ac",
    "airconditioner": "ac",
    "acairconditioner": "ac",
    "airconditionerac": "ac",
    "emas": "emas",
    "perhiasan": "emas",
    "emasperhiasan": "emas",
    "emasperhiasanmin10gram": "emas",
    "komputer": "komputer",
    "laptop": "komputer",
    "tablet": "komputer",
    "komputerlaptoptablet": "komputer",
    "lemaries": "lemari_es",
    "lemarieskulkas": "lemari_es",
    "kulkas": "lemari_es",
    "mobil": "mobil",
    "sepeda": "sepeda",
    "sepedamotor": "sepeda_motor",
    "motor": "sepeda_motor",
    "smartphone": "smartphone",
    "hp": "smartphone",
    "televisi": "televisi",
    "tv": "televisi",
    "tvflat": "televisi",
    "televisilayardatar": "televisi",
    "televisilayardatarmin30inci": "televisi",
    # Additional assets
    "kapal": "kapal_perahu_motor",
    "perahumotor": "kapal_perahu_motor",
    "kapalperahumotor": "kapal_perahu_motor",
    "pemanasair": "pemanas_air",
    "waterheater": "pemanas_air",
    "pemanasairwaterheater": "pemanas_air",
    "perahu": "perahu",
    "tabunggas": "tabung_gas",
    "tabunggas55kg": "tabung_gas",
    "tabunggas55kgataulebih": "tabung_gas",
    "telepon": "telepon_rumah",
    "teleponrumah": "telepon_rumah",
    "teleponrumahpstn": "telepon_rumah",
    "pstn": "telepon_rumah",
}

# Desil labels
DESIL_LABELS = ["DESIL_1", "DESIL_2", "DESIL_3", "DESIL_4", "DESIL_5", "DESIL_6_10", "DESIL_BELUM_DITENTUKAN"]

# Columns to drop from Excel output
COLS_TO_DROP = {
    'id_keluarga', 'id_keluarga_parent', 'idsemesta', 'ID_KELUARGA', 'IDSEMESTA',
    'id_keluarga_aset', 'id_keluarga_parent_pbi',
    'id_keluarga_kyc', 'no_kk_kyc', 'nik_input', 'id_jenis_kelamin',
    'id_status_perkawinan', 'id_hub_keluarga', 'id_keluarga_pbi',
    'id_deleted', 'alasan_tolak_meninggal', 'nama_input',
    'no_prop', 'no_kab', 'no_kec', 'no_kel',
    'id_pekerjaan_utama', 'id_keluarga_parent_kyc', 'idsemesta_pbi',
    'pkh_flag', 'bpnt_flag', 'pbi_flag'
}

# Text columns (preserve leading zeros)
TEXT_COLS = {'nik', 'no_kk', 'NIK', 'NO_KK', 'nik_anggota', 'nomor_kartu', 'nomor_kks', 'nomor_pkh'}

# Date columns
DATE_COLS = {'tgl_lahir', 'tanggal_lahir', 'TGL_LAHIR', 'tanggal_pencairan', 'tanggal_pembayaran', 'tanggal'}
