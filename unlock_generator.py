#!/usr/bin/env python3
"""
unlock_generator.py - Generates and verifies 3x4 character unlock codes
Code format: ABCD-EFGH-IJKL
"""

import hmac
import hashlib
import base64
import re
from typing import Optional

def generate_raw_code(secret_salt: str, package: str, tx_id: str) -> str:
    """Generate HMAC-SHA256 based code"""
    message = f"{package}|{tx_id}"
    return hmac.new(
        secret_salt.encode(),
        message.encode(),
        hashlib.sha256
    ).hexdigest()

def format_code(raw_hex: str) -> str:
    """Convert hex to 3x4 uppercase alphanumeric groups"""
    # Take first 24 hex chars (12 bytes) and encode as base32
    data = bytes.fromhex(raw_hex[:24])
    b32 = base64.b32encode(data).decode().rstrip('=')
    # Take first 12 chars and format
    code = b32[:12]
    return f"{code[:4]}-{code[4:8]}-{code[8:12]}"

def generate_unlock_code(secret_salt: str, package: str, tx_id: str) -> str:
    """Generate formatted unlock code"""
    raw = generate_raw_code(secret_salt, package, tx_id)
    return format_code(raw)

def verify_unlock_code(code: str, secret_salt: str, package: str, tx_id: str) -> bool:
    """Verify if a code matches package + tx_id"""
    try:
        # Normalize code
        code = code.upper().replace("-", "")
        if len(code) != 12:
            return False

        expected = generate_unlock_code(secret_salt, package, tx_id)
        # Compare in constant time
        return hmac.compare_digest(code, expected.replace("-", ""))
    except:
        return False

def determine_package_from_code(code: str, secret_salt: str, tx_id: str) -> Optional[str]:
    """Auto-detect package from code"""
    if verify_unlock_code(code, secret_salt, "BASIC", tx_id):
        return "BASIC"
    if verify_unlock_code(code, secret_salt, "PRO", tx_id):
        return "PRO"
    return None

# Test
if __name__ == "__main__":
    SALT = "TEST_SALT_123"
    TX = "TX-3F9C2A77CE"

    basic_code = generate_unlock_code(SALT, "BASIC", TX)
    pro_code = generate_unlock_code(SALT, "PRO", TX)

    print(f"BASIC: {basic_code}")
    print(f"PRO:   {pro_code}")

    assert determine_package_from_code(basic_code, SALT, TX) == "BASIC"
    assert determine_package_from_code(pro_code, SALT, TX) == "PRO"
    assert determine_package_from_code("XXXX-XXXX-XXXX", SALT, TX) is None
    print("✓ All tests passed")