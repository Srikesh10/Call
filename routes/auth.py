"""
routes/auth.py — Authentication, user profile, and Twilio provisioning.
"""
import os
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from dependencies import get_current_user
from backend.supabase_client import supabase
from twilio_provisioning import get_provisioner

logger = logging.getLogger("alora.auth")

router = APIRouter(tags=["Auth"])


# ── Pydantic Models ──────────────────────────────────────────────────────────

class GoogleToken(BaseModel):
    provider_token: str
    provider_refresh_token: str | None = None
    expires_in: int | None = None
    token_type: str | None = None

class TwilioProvisionRequest(BaseModel):
    email: Optional[str] = None


# ── User Profile ─────────────────────────────────────────────────────────────

@router.get("/api/user/profile")
async def get_user_profile(user: dict = Depends(get_current_user)):
    """Returns the authenticated user's profile data."""
    try:
        email = getattr(user, 'email', "Unknown")
        metadata = getattr(user, 'user_metadata', {})

        return {
            "status": "success",
            "email": email,
            "full_name": metadata.get("full_name") or metadata.get("name") or "User",
            "avatar_url": metadata.get("avatar_url") or metadata.get("picture"),
            "metadata": metadata
        }
    except Exception as e:
        logger.error(f"Profile fetch error: {e}")
        return {"email": getattr(user, 'email', 'unknown'), "full_name": "User", "avatar_url": None}


# ── Google Token Sync ────────────────────────────────────────────────────────

@router.post("/api/auth/sync_google")
async def sync_google_token(token_data: GoogleToken, user: dict = Depends(get_current_user)):
    """Receives Google Tokens from Frontend and stores them securely."""
    from backend.supabase_client import supabase_adapter

    if not token_data.provider_refresh_token:
        raise HTTPException(status_code=400, detail="A valid refresh token is required.")

    success = supabase_adapter.store_google_refresh_token(
        user_id=user.id,
        refresh_token=token_data.provider_refresh_token
    )

    if success:
        return {"status": "success", "message": "Credentials Vaulted"}
    else:
        raise HTTPException(status_code=500, detail="Failed to vault credentials")


# ── Twilio Provisioning ─────────────────────────────────────────────────────

@router.post("/api/auth/provision-twilio")
async def provision_twilio_subaccount(data: TwilioProvisionRequest, user: dict = Depends(get_current_user)):
    """Provision a Twilio subaccount for the authenticated user."""
    from backend.supabase_client import supabase_adapter

    try:
        existing = supabase_adapter.get_twilio_account(user.id)
    except Exception as e:
        logger.error(f"Twilio provision DB check failed: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    if existing:
        return {
            "status": "exists",
            "message": "Twilio subaccount already provisioned",
            "subaccount_sid": existing["subaccount_sid"],
            "friendly_name": existing["friendly_name"]
        }

    try:
        provisioner = get_provisioner()
        subaccount_data = provisioner.create_subaccount(
            user_id=user.id,
            email=data.email or user.email
        )

        success = supabase_adapter.save_twilio_account(
            user_id=user.id,
            subaccount_data=subaccount_data
        )

        if not success:
            raise HTTPException(status_code=500, detail="Failed to save Twilio account to database")

        logger.info(f"Twilio subaccount provisioned for user {user.id}")
        return {
            "status": "success",
            "message": "Twilio subaccount provisioned successfully",
            "subaccount_sid": subaccount_data["subaccount_sid"],
            "friendly_name": subaccount_data["friendly_name"]
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Twilio provisioning failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Twilio provisioning failed: {str(e)}")


@router.get("/api/twilio/status")
async def get_twilio_status(user: dict = Depends(get_current_user)):
    """Retrieve Twilio subaccount status for the current user."""
    from backend.supabase_client import supabase_adapter

    account = supabase_adapter.get_twilio_account(user.id)

    if not account:
        return {"status": "not_provisioned", "message": "No Twilio subaccount found"}

    return {
        "status": account["status"],
        "subaccount_sid": account["subaccount_sid"],
        "friendly_name": account["friendly_name"],
        "created_at": account["created_at"]
    }
