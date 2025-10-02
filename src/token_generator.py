#!/usr/bin/env python3
"""
JWT Token Generator for Airflow API Authentication
Outputs only the token to stdout for easy capture by shell scripts.
"""

import jwt
import os
import sys
from datetime import datetime, timedelta, timezone

def generate_jwt_token(username: str, secret_key: str, audience: str, expiration_sec: int):
    """Generates a signed JWT token."""
    expiration_time = datetime.now(timezone.utc) + timedelta(seconds=expiration_sec)
    
    payload = {
        "username": username,
        "aud": audience,
        "exp": int(expiration_time.timestamp()),
    }
    
    token = jwt.encode(payload, secret_key, algorithm="HS256")
    
    return token, expiration_time


if __name__ == "__main__":
    try:
        # Get configuration from environment variables
        USERNAME = os.getenv('AIRFLOW_USERNAME', 'admin')
        SECRET_KEY = os.getenv('AIRFLOW_JWT_SECRET', 'credit-card-fraud-detection_a176c0')
        AUDIENCE = "urn:airflow.apache.org:api"
        EXPIRATION_SECONDS = 86400  # 24 hours
        
        if not SECRET_KEY:
            print("ERROR: AIRFLOW_JWT_SECRET not set", file=sys.stderr)
            sys.exit(1)
        
        bearer_token, expires_at = generate_jwt_token(
            USERNAME,
            SECRET_KEY,
            AUDIENCE,
            EXPIRATION_SECONDS
        )
        
        # Log details to stderr (won't be captured by shell)
        print(f"Token generated for user: {USERNAME}", file=sys.stderr)
        print(f"Expires: {expires_at.strftime('%Y-%m-%d %H:%M:%S %Z')}", file=sys.stderr)
        
        # Output ONLY the token to stdout (can be captured)
        print(bearer_token)
        
    except Exception as e:
        print(f"ERROR generating token: {e}", file=sys.stderr)
        sys.exit(1)