"""
Entity Deduplicator Module
Handles deduplication of family records based on unique id_keluarga.
"""

from typing import Dict, List, Tuple, Optional, Any
import json
import base64
import hmac
import hashlib

from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad


def safe_b64decode(s: str) -> bytes:
    """Safely decode base64 string with padding fix."""
    s = (s or "").strip().replace("\n", "").replace(" ", "")
    s += "=" * (-len(s) % 4)
    return base64.b64decode(s)


def decrypt_entity(entity_b64: str, aes_key_b64: str) -> Any:
    """Decrypt API response entity."""
    outer = json.loads(safe_b64decode(entity_b64).decode("utf-8"))
    iv = safe_b64decode(outer["iv"])
    ciphertext = safe_b64decode(outer["value"])
    mac_expected = outer.get("mac")
    key = safe_b64decode(aes_key_b64)

    # Verify MAC
    mac_data = base64.b64encode(iv).decode() + base64.b64encode(ciphertext).decode()
    mac_calc = hmac.new(key, mac_data.encode(), hashlib.sha256).hexdigest()
    if mac_expected and mac_calc != mac_expected:
        raise ValueError("MAC mismatch")

    # Decrypt
    cipher = AES.new(key, AES.MODE_CBC, iv)
    plaintext = unpad(cipher.decrypt(ciphertext), AES.block_size)

    try:
        return json.loads(plaintext.decode("utf-8"))
    except:
        return plaintext.decode("utf-8")


def extract_id_keluarga(family_row: Dict) -> Optional[str]:
    """
    Extract id_keluarga from a family record.
    Handles various field name variations.
    """
    for key in ["id_keluarga", "ID_KELUARGA", "id_keluarga_parent"]:
        if key in family_row and family_row[key]:
            return str(family_row[key])
    return None


def deduplicate_families(families: List[Dict]) -> Tuple[List[Dict], int]:
    """
    Remove families with duplicate id_keluarga, keeping the first occurrence.

    Args:
        families: List of family dictionaries

    Returns:
        Tuple of (unique_families, duplicates_removed_count)
    """
    seen_ids: set = set()
    unique_families: List[Dict] = []
    duplicates_removed = 0

    for family in families:
        id_keluarga = extract_id_keluarga(family)

        if id_keluarga is None:
            # No ID found, keep the record but log warning
            unique_families.append(family)
            continue

        if id_keluarga in seen_ids:
            duplicates_removed += 1
            continue

        seen_ids.add(id_keluarga)
        unique_families.append(family)

    return unique_families, duplicates_removed


def preprocess_families(
    entity_payloads: List[str],
    aes_key: str
) -> Tuple[List[Dict], int, int]:
    """
    Decrypt entity payloads, extract families, and deduplicate by id_keluarga.

    Args:
        entity_payloads: List of encrypted entity payload strings
        aes_key: Base64-encoded AES key for decryption

    Returns:
        Tuple of (unique_families, total_raw_count, duplicates_removed)
    """
    all_families: List[Dict] = []

    for payload in entity_payloads:
        try:
            decrypted = decrypt_entity(payload, aes_key)

            # Extract rows from decrypted data
            if isinstance(decrypted, dict):
                rows = decrypted.get("data", [])
            elif isinstance(decrypted, list):
                rows = decrypted
            else:
                rows = []

            # Normalize id_keluarga_parent for each row
            for row in rows:
                if isinstance(row, dict):
                    idk = extract_id_keluarga(row)
                    if idk:
                        row["id_keluarga_parent"] = idk
                    all_families.append(row)

        except Exception as e:
            # Log but continue processing other payloads
            print(f"[Deduplicator] Failed to process payload: {e}")
            continue

    total_raw = len(all_families)
    unique_families, duplicates_removed = deduplicate_families(all_families)

    return unique_families, total_raw, duplicates_removed


def get_deduplication_summary(total_raw: int, duplicates_removed: int) -> str:
    """
    Generate a user-friendly deduplication summary message.

    Args:
        total_raw: Total families before deduplication
        duplicates_removed: Number of duplicates removed

    Returns:
        Human-readable summary string
    """
    unique_count = total_raw - duplicates_removed

    if duplicates_removed == 0:
        return f"✅ {unique_count} keluarga ditemukan (tidak ada duplikat)"
    else:
        return (
            f"✅ {unique_count} keluarga unik ditemukan "
            f"(dihapus {duplicates_removed} duplikat dari {total_raw} total)"
        )
