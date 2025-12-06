"""
Unit tests for Defensive Programming Utilities
"""

import unittest
from core.defensive import PIISanitizer, validate_bearer_token, validate_id_keluarga

class TestDefensive(unittest.TestCase):

    def test_pii_sanitizer_nik(self):
        """Test NIK sanitization."""
        text = "My NIK is 1234567890123456"
        sanitized = PIISanitizer.sanitize(text)
        self.assertIn("1234********3456", sanitized)
        self.assertNotIn("1234567890123456", sanitized)

    def test_pii_sanitizer_phone(self):
        """Test phone number sanitization."""
        text = "Call me at 08123456789"
        sanitized = PIISanitizer.sanitize(text)
        self.assertIn("***MASKED***", sanitized)
        self.assertNotIn("08123456789", sanitized)

    def test_pii_sanitizer_email(self):
        """Test email sanitization."""
        text = "Email: test.user@example.com"
        sanitized = PIISanitizer.sanitize(text)
        self.assertIn("te***@***", sanitized)
        self.assertNotIn("test.user@example.com", sanitized)

    def test_sanitize_html(self):
        """Test HTML sanitization."""
        text = "<script>alert('xss')</script>"
        sanitized = PIISanitizer.sanitize_html(text)
        self.assertEqual(sanitized, "&lt;script&gt;alert(&#x27;xss&#x27;)&lt;/script&gt;")

    def test_sanitize_pdf_text(self):
        """Test PDF text sanitization."""
        text = "Hello\x00World\nTest"
        sanitized = PIISanitizer.sanitize_pdf_text(text)
        self.assertEqual(sanitized, "HelloWorld\nTest")

    def test_validate_bearer_token(self):
        """Test bearer token validation."""
        valid_token = "Bearer " + "a" * 20
        self.assertTrue(validate_bearer_token(valid_token))

        invalid_token = "short"
        self.assertFalse(validate_bearer_token(invalid_token))

        invalid_chars = "Bearer token!@#"
        self.assertFalse(validate_bearer_token(invalid_chars))

    def test_validate_id_keluarga(self):
        """Test ID Keluarga validation."""
        self.assertTrue(validate_id_keluarga("12345678"))
        self.assertTrue(validate_id_keluarga("550e8400-e29b-41d4-a716-446655440000"))
        self.assertFalse(validate_id_keluarga("123"))
        self.assertFalse(validate_id_keluarga("invalid-uuid"))

if __name__ == '__main__':
    unittest.main()
