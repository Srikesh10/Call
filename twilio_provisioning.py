"""
Twilio Subaccount Auto-Provisioning Service

Handles automatic creation of Twilio subaccounts on user signup with
encrypted credential storage.
"""

import os
import base64
from typing import Dict, Optional
from cryptography.fernet import Fernet
from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException


class TwilioProvisioner:
    """Manages Twilio subaccount creation and credential encryption."""
    
    def __init__(self):
        # Load master account credentials
        self.master_sid = os.environ.get("TWILIO_MASTER_SID")
        self.master_auth_token = os.environ.get("TWILIO_MASTER_AUTH_TOKEN")
        
        if not self.master_sid or not self.master_auth_token:
            raise ValueError("TWILIO_MASTER_SID and TWILIO_MASTER_AUTH_TOKEN must be set")
        
        # Initialize Twilio client
        self.client = Client(self.master_sid, self.master_auth_token)
        
        # Load or generate encryption key
        encryption_key = os.environ.get("TWILIO_ENCRYPTION_KEY")
        if not encryption_key:
            raise ValueError("TWILIO_ENCRYPTION_KEY must be set (generate with: python -c \"import secrets, base64; print(base64.b64encode(secrets.token_bytes(32)).decode())\")")
        
        self.cipher = Fernet(encryption_key.encode())
    
    def create_subaccount(self, user_id: str, email: Optional[str] = None) -> Dict[str, str]:
        """
        Create a new Twilio subaccount for a user.
        
        Args:
            user_id: User UUID from Supabase
            email: Optional user email for friendly name
            
        Returns:
            Dictionary containing:
                - subaccount_sid: The subaccount SID
                - encrypted_auth_token: Fernet-encrypted auth token
                - friendly_name: Human-readable name
                
        Raises:
            TwilioRestException: If subaccount creation fails
            Exception: For other errors
        """
        try:
            # Generate friendly name
            friendly_name = f"user_{user_id[:8]}"
            if email:
                friendly_name = f"{email}_{user_id[:8]}"
            
            # Create subaccount via Twilio API
            print(f"[TWILIO] Creating subaccount: {friendly_name}")
            subaccount = self.client.api.accounts.create(
                friendly_name=friendly_name
            )
            
            # Extract credentials
            subaccount_sid = subaccount.sid
            auth_token = subaccount.auth_token
            
            print(f"[TWILIO] Subaccount created: {subaccount_sid}")
            
            # Encrypt auth token
            encrypted_token = self.encrypt_token(auth_token)
            
            return {
                "subaccount_sid": subaccount_sid,
                "encrypted_auth_token": encrypted_token,
                "friendly_name": friendly_name,
                "status": "active"
            }
            
        except TwilioRestException as e:
            print(f"[TWILIO ERROR] Failed to create subaccount: {e.msg}")
            raise Exception(f"Twilio subaccount creation failed: {e.msg}")
        except Exception as e:
            print(f"[TWILIO ERROR] Unexpected error: {str(e)}")
            raise
    
    def encrypt_token(self, token: str) -> str:
        """
        Encrypt an auth token using Fernet symmetric encryption.
        
        Args:
            token: Plaintext auth token
            
        Returns:
            Base64-encoded encrypted token
        """
        encrypted = self.cipher.encrypt(token.encode())
        return encrypted.decode()
    
    def decrypt_token(self, encrypted_token: str) -> str:
        """
        Decrypt an auth token.
        
        Args:
            encrypted_token: Base64-encoded encrypted token
            
        Returns:
            Plaintext auth token
        """
        decrypted = self.cipher.decrypt(encrypted_token.encode())
        return decrypted.decode()
    
    def get_subaccount_client(self, subaccount_sid: str, encrypted_token: str) -> Client:
        """
        Create a Twilio client for a specific subaccount.
        
        Args:
            subaccount_sid: The subaccount SID
            encrypted_token: Encrypted auth token from database
            
        Returns:
            Twilio Client authenticated with subaccount credentials
        """
        auth_token = self.decrypt_token(encrypted_token)
        return Client(subaccount_sid, auth_token)
    
    def suspend_subaccount(self, subaccount_sid: str) -> bool:
        """
        Suspend a subaccount (change status to suspended).
        
        Args:
            subaccount_sid: The subaccount SID to suspend
            
        Returns:
            True if successful
        """
        try:
            self.client.api.accounts(subaccount_sid).update(
                status='suspended'
            )
            print(f"[TWILIO] Subaccount suspended: {subaccount_sid}")
            return True
        except TwilioRestException as e:
            print(f"[TWILIO ERROR] Failed to suspend subaccount: {e.msg}")
            return False
    
    def reactivate_subaccount(self, subaccount_sid: str) -> bool:
        """
        Reactivate a suspended subaccount.
        
        Args:
            subaccount_sid: The subaccount SID to reactivate
            
        Returns:
            True if successful
        """
        try:
            self.client.api.accounts(subaccount_sid).update(
                status='active'
            )
            print(f"[TWILIO] Subaccount reactivated: {subaccount_sid}")
            return True
        except TwilioRestException as e:
            print(f"[TWILIO ERROR] Failed to reactivate subaccount: {e.msg}")
            return False


# Singleton instance
_provisioner = None

def get_provisioner() -> TwilioProvisioner:
    """Get or create the TwilioProvisioner singleton."""
    global _provisioner
    if _provisioner is None:
        _provisioner = TwilioProvisioner()
    return _provisioner
