#!/usr/bin/env python3
"""
VAPID Key Generator for Web Push Notifications

This script generates a VAPID keypair using the py-vapid library.
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

from py_vapid import Vapid


def generate_vapid_keys():
    print('=' * 70)
    print('VAPID Key Generator for Web Push Notifications')
    print('=' * 70)
    print()

    # Generate VAPID keypair
    vapid = Vapid()
    vapid.generate_keys()

    # Save private key to PEM file
    vapid.save_key('vapid_private.pem')
    
    # Get public key in URL-safe base64 format (without padding)
    # The save_public_key method returns the key in the correct format
    public_key_b64 = vapid.save_public_key('vapid_public.txt')

    print('✅ VAPID keys generated successfully!')
    print()
    print('-' * 70)
    print('Files created:')
    print('-' * 70)
    print()
    print('  📄 vapid_private.pem  - Private key (keep secret!)')
    print('  📄 vapid_public.txt   - Public key (URL-safe base64)')
    print()
    print('-' * 70)
    print('Add this to your environment variables (.env file):')
    print('-' * 70)
    print()
    print('VAPID_CLAIM_EMAIL=mailto:your-email@example.com')
    print()
    print('-' * 70)
    print()
    print('⚠️  IMPORTANT SECURITY NOTES:')
    print('   1. Keep vapid_private.pem secret - never commit it to git')
    print('   2. Add vapid_*.pem and vapid_*.txt to your .gitignore file')
    print('   3. The application will read these files at runtime')
    print('   4. Update VAPID_CLAIM_EMAIL with your actual contact email')
    print()
    print('=' * 70)


if __name__ == '__main__':
    try:
        generate_vapid_keys()
    except Exception as e:
        print('❌ Error generating VAPID keys:', e)
        print()
        print('Make sure `py-vapid` is installed:')
        print('    pip install py-vapid')
