import os
from fastapi import Depends, HTTPException, status, Header
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from backend.supabase_client import supabase

security = HTTPBearer()

# DEBUG_MODE allows the test token only in local dev.
# Set DEBUG_MODE=false in production .env to disable it completely.
DEBUG_MODE = os.environ.get("DEBUG_MODE", "false").lower() == "true"

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security),
                     x_user_id: str = Header(default=None),
                     x_user_email: str = Header(default=None)):
    """
    Verifies the Bearer Token using Supabase JWT validation.

    In DEBUG_MODE only: accepts 'oauth_success_token' as a test bypass.
    Both x-user-id and x-user-email headers MUST be present — no hardcoded fallback.
    In production (DEBUG_MODE=false): only real Supabase JWTs are accepted.
    """
    token = credentials.credentials

    # ── TEST BYPASS (DEBUG_MODE only) ─────────────────────────────────────────
    if token == "oauth_success_token":
        if not DEBUG_MODE:
            # Backdoor is disabled in production — reject immediately
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Test token not allowed in production."
            )

        # DEBUG_MODE: both headers required — no hardcoded fallback
        if not x_user_id or not x_user_email:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Test token requires x-user-id and x-user-email headers."
            )

        class OAuthUser:
            def __init__(self, user_id, email):
                self.id = user_id
                self.email = email
                self.aud = "authenticated"
                self.role = "authenticated"
                self.app_metadata = {"provider": "google"}
                self.user_metadata = {"provider": "google"}
                self.created_at = "2025-01-01T00:00:00Z"

        print(f"[AUTH] Debug user: {x_user_id} ({x_user_email})")
        return OAuthUser(x_user_id, x_user_email)

    # ── PRODUCTION: Supabase JWT validation ────────────────────────────────────
    try:
        user = supabase.auth.get_user(token)
        if not user:
            raise HTTPException(status_code=401, detail="Invalid token.")
        return user.user
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=401, detail="Authentication failed.")
