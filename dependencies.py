from fastapi import Depends, HTTPException, status, Header
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from backend.supabase_client import supabase

security = HTTPBearer()

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security), 
                     x_user_id: str = Header(default=None), 
                     x_user_email: str = Header(default=None)):
    """Verifies the Bearer Token using Supabase or OAuth success token"""
    token = credentials.credentials
    # DEBUG TOKEN
    if not token or len(token) < 10:
        print(f"[AUTH DEBUG] Invalid Token Received: {token}")
    else:
        print(f"[AUTH DEBUG] Verifying Token: {token[:10]}... (Length: {len(token)})")

    # Handle OAuth success token - get user info from headers
    if token == "oauth_success_token":
        print("[AUTH DEBUG] OAuth Success Token Detected")
        print(f"[AUTH DEBUG] OAuth User from headers: ID={x_user_id}, Email={x_user_email}")
        
        # Create a user object for OAuth users
        class OAuthUser:
            def __init__(self, user_id, email):
                self.id = user_id
                self.email = email
                self.aud = "authenticated"
                self.role = "authenticated"
                self.app_metadata = {"provider": "google"}
                self.user_metadata = {"provider": "google"}
                self.created_at = "2025-01-01T00:00:00Z"
        
        if x_user_id and x_user_email:
            return OAuthUser(x_user_id, x_user_email)
        else:
            # Fallback: Use known user ID (for dev/testing when headers not sent)
            # In production, this should fetch from session or reject
            print("[AUTH DEBUG] Using fallback user ID (headers missing)")
            fallback_id = "655b1b48-66b6-4455-9a92-3fcac8c377eb"
            fallback_email = "rahulsamineni1234@gmail.com"
            return OAuthUser(fallback_id, fallback_email)

    try:
        user = supabase.auth.get_user(token)
        if not user:
             raise HTTPException(status_code=401, detail="Invalid Supabase Token")
        return user.user
    except Exception as e:
        print(f"[AUTH ERROR] Verification Failed: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=401, detail=str(e))
