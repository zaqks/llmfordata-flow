#!/usr/bin/env python3
"""
VAPID Key Generator for Web Push Notifications

This script generates an EC keypair (SECP256R1) and prints:
- VAPID_PRIVATE_KEY (PEM) - keep this secret
- VAPID_PUBLIC_KEY (URL-safe base64, no padding) - used by browser subscribe
- VAPID_CLAIM_EMAIL - example value

Usage:
    python generate_vapid_keys.py

The generated `VAPID_PUBLIC_KEY` is the uncompressed EC public key (0x04 | X | Y)
encoded with URL-safe base64 (padding removed), which is what the browser expects
for `applicationServerKey` when subscribing to push.

The `VAPID_PRIVATE_KEY` is printed in PEM (PKCS8) format and can be used by
`pywebpush` as `vapid_private_key`.
"""

import base64
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization


def urlsafe_b64encode_no_padding(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode('utf-8').rstrip('=')


def generate_vapid_keys():
    print('=' * 70)
    print('VAPID Key Generator for Web Push Notifications')
    print('=' * 70)
    print()

    # Generate EC private key (SECP256R1)
    private_key = ec.generate_private_key(ec.SECP256R1())

    # Serialize private key to PEM (PKCS8)
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    ).decode('utf-8')

    # Get public key in uncompressed point format: 0x04 || X || Y
    public_key = private_key.public_key()
    numbers = public_key.public_numbers()
    x = numbers.x.to_bytes(32, 'big')
    y = numbers.y.to_bytes(32, 'big')
    uncompressed_pk = b'\x04' + x + y

    public_key_b64 = urlsafe_b64encode_no_padding(uncompressed_pk)

    print('✅ VAPID keys generated successfully!')
    print()
    print('-' * 70)
    print('Add these to your environment variables (.env file):')
    print('-' * 70)
    print()
    print('VAPID_PRIVATE_KEY=' + private_pem)
    print('VAPID_PUBLIC_KEY=' + public_key_b64)
    print('VAPID_CLAIM_EMAIL=mailto:your-email@example.com')
    print()
    print('-' * 70)
    print()
    print('⚠️  IMPORTANT SECURITY NOTES:')
    print('   1. Keep VAPID_PRIVATE_KEY secret - never commit it to git')
    print('   2. Add .env to your .gitignore file')
    print('   3. The VAPID_PUBLIC_KEY will be sent to browsers')
    print('   4. Update VAPID_CLAIM_EMAIL with your actual contact email')
    print()
    print('=' * 70)


if __name__ == '__main__':
    try:
        generate_vapid_keys()
    except Exception as e:
        print('❌ Error generating VAPID keys:', e)
        print()
        print('Make sure `cryptography` is installed:')
        print('    pip install cryptography')
