import os
from pywebpush import webpush, WebPushException
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

def test_vapid():
    print("Testing VAPID key loading...")
    
    key_path = 'vapid_private.pem' # Relative because I will run it from services/api
    if not os.path.exists(key_path):
        key_path = '/app/vapid_private.pem' # docker path
    
    if not os.path.exists(key_path):
        print(f"Key file not found at {key_path}")
        return

    print(f"Found key at {key_path}")

    try:
        with open(key_path, 'rb') as f:
            private_key_pem = f.read()
        
        print("Loaded key content successfully.")
        
        # Validate key
        private_key = serialization.load_pem_private_key(
            private_key_pem,
            password=None,
            backend=default_backend()
        )
        print("Validated key with cryptography successfully.")
        
        vapid_private_key_str = private_key_pem.decode('utf-8')
        
    except Exception as e:
        print(f"Error manually loading key: {e}")
        return

    # Mock subscription
    subscription_info = {
        "endpoint": "https://fcm.googleapis.com/fcm/send/fJw...",
        "keys": {
            "p256dh": "BM...",
            "auth": "WK..."
        }
    }
    
    vapid_claims = {"sub": "mailto:test@example.com"}
    
    # Test 1: Pass string content
    print("\nTest 1: Passing key as string content to webpush...")
    try:
        webpush(
            subscription_info=subscription_info,
            data="test",
            vapid_private_key=vapid_private_key_str,
            vapid_claims=vapid_claims
        )
        print("Test 1 Result: Success (or at least got past key loading)")
    except WebPushException as e:
        # Expected to fail sending because of fake endpoint, but check error message
        print(f"Test 1 WebPushException: {e}")
    except Exception as e:
        print(f"Test 1 Unexpected Exception: {e}")

    # Test 2: Pass file path
    print("\nTest 2: Passing key as file path to webpush...")
    try:
        webpush(
            subscription_info=subscription_info,
            data="test",
            vapid_private_key=key_path,
            vapid_claims=vapid_claims
        )
        print("Test 2 Result: Success (or at least got past key loading)")
    except WebPushException as e:
        print(f"Test 2 WebPushException: {e}")
    except Exception as e:
        print(f"Test 2 Unexpected Exception: {e}")

    # Test 3: Pass private_key object
    print("\nTest 3: Passing private_key object to webpush...")
    try:
        webpush(
            subscription_info=subscription_info,
            data="test",
            vapid_private_key=private_key,
            vapid_claims=vapid_claims
        )
        print("Test 3 Result: Success")
    except Exception as e:
        print(f"Test 3 Exception: {e}")

if __name__ == "__main__":
    test_vapid()
