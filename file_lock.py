#!/usr/bin/env python3
"""
file_lock.py - Dual-lock encryption system
Each file gets TWO encrypted versions: .locked_basic and .locked_pro
"""

import os
import json
import hmac
import hashlib
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend
import secrets
from typing import Dict, Tuple

# Constants
AES_KEY_SIZE = 32  # 256-bit
AES_BLOCK_SIZE = 16
HMAC_DIGEST_SIZE = 32

def derive_aes_key(secret_salt: str, package_type: str, tx_id: str) -> bytes:
    """Derive AES key using SHA256(secret_salt | package_type | tx_id)"""
    key_material = f"{secret_salt}|{package_type}|{tx_id}"
    return hashlib.sha256(key_material.encode()).digest()

def encrypt_data(plaintext: bytes, key: bytes, tx_id: str = None) -> Dict[str, str]:
    """Encrypt with AES-256-CBC + HMAC-SHA256"""
    # Generate random IV
    iv = secrets.token_bytes(AES_BLOCK_SIZE)

    # Pad plaintext
    padder = padding.PKCS7(AES_BLOCK_SIZE * 8).padder()
    padded_data = padder.update(plaintext) + padder.finalize()

    # Encrypt
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    encryptor = cipher.encryptor()
    ciphertext = encryptor.update(padded_data) + encryptor.finalize()

    # Compute HMAC over iv + ciphertext
    mac_data = iv + ciphertext
    mac = hmac.new(key, mac_data, hashlib.sha256).hexdigest()

    # Return JSON-serializable structure (include tx_id for unlock mode)
    result = {
        "iv": iv.hex(),
        "ciphertext": ciphertext.hex(),
        "mac": mac
    }
    if tx_id:
        result["tx_id"] = tx_id
    return result

def decrypt_data(encrypted_json: Dict[str, str], key: bytes) -> bytes:
    """Decrypt and verify HMAC"""
    # Extract components
    iv = bytes.fromhex(encrypted_json["iv"])
    ciphertext = bytes.fromhex(encrypted_json["ciphertext"])
    expected_mac = encrypted_json["mac"]

    # Verify HMAC
    mac_data = iv + ciphertext
    actual_mac = hmac.new(key, mac_data, hashlib.sha256).hexdigest()

    if not hmac.compare_digest(actual_mac, expected_mac):
        raise ValueError("MAC verification failed - data tampered or wrong key")

    # Decrypt
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    decryptor = cipher.decryptor()
    padded_plaintext = decryptor.update(ciphertext) + decryptor.finalize()

    # Unpad
    unpadder = padding.PKCS7(AES_BLOCK_SIZE * 8).unpadder()
    return unpadder.update(padded_plaintext) + unpadder.finalize()

def encrypt_dual_versions(
    file_bytes: bytes,
    file_name: str,
    out_folder: str,
    secret_salt: str,
    tx_id: str
) -> Tuple[str, str]:
    """
    Create TWO encrypted versions of a file:
    - .locked_basic (can be unlocked with BASIC package)
    - .locked_pro (can be unlocked with PRO package)
    """
    os.makedirs(out_folder, exist_ok=True)

    # BASIC version (include tx_id in encrypted file for unlock mode)
    basic_key = derive_aes_key(secret_salt, "BASIC", tx_id)
    basic_encrypted = encrypt_data(file_bytes, basic_key, tx_id)
    basic_path = os.path.join(out_folder, f"{file_name}.locked_basic")
    with open(basic_path, "w") as f:
        json.dump(basic_encrypted, f)

    # PRO version (include tx_id in encrypted file for unlock mode)
    pro_key = derive_aes_key(secret_salt, "PRO", tx_id)
    pro_encrypted = encrypt_data(file_bytes, pro_key, tx_id)
    pro_path = os.path.join(out_folder, f"{file_name}.locked_pro")
    with open(pro_path, "w") as f:
        json.dump(pro_encrypted, f)

    return basic_path, pro_path

def decrypt_locked_file(
    locked_path: str,
    secret_salt: str,
    tx_id: str,
    package: str
) -> bytes:
    """Decrypt a .locked_basic or .locked_pro file"""
    with open(locked_path, "r") as f:
        encrypted_data = json.load(f)

    # Use tx_id from file if not provided (for unlock mode)
    actual_tx_id = tx_id
    if not actual_tx_id and "tx_id" in encrypted_data:
        actual_tx_id = encrypted_data["tx_id"]
        print(f"[UNLOCK] Using tx_id from file: {actual_tx_id}")

    if not actual_tx_id:
        raise ValueError("No tx_id available - cannot derive decryption key")

    key = derive_aes_key(secret_salt, package, actual_tx_id)
    return decrypt_data(encrypted_data, key)

def unlock_to_file(
    locked_path: str,
    output_path: str,
    secret_salt: str,
    tx_id: str,
    package: str
) -> None:
    """Decrypt and save to plaintext file"""
    plaintext = decrypt_locked_file(locked_path, secret_salt, tx_id, package)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "wb") as f:
        f.write(plaintext)