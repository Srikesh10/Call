"""
Twilio Subaccount Management Module

Handles automatic creation of Twilio subaccounts for new users,
with secure token encryption and database storage.
"""

import os
import requests
from typing import Dict, Optional, Tuple
from cryptography.fernet import Fernet
import base64
import hashlib
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TwilioSubaccountManager:
    """Manages Twilio subaccount creation and credential storage"""
    
    def __init__(self):
        self.master_sid = os.environ.get("TWILIO_MASTER_SID")
        self.master_auth_token = os.environ.get("TWILIO_MASTER_AUTH_TOKEN")
        self.twilio_api_base = "https://api.twilio.com/2010-04-01"
        
        if not self.master_sid or not self.master_auth_token:
            logger.warning("[Twilio] Master credentials not found in environment")
    
    def _get_encryption_key(self) -> bytes:
        """
        Generate or retrieve encryption key from environment.
        Uses a deterministic key based on a secret, or generates from env.
        """
        # Try to get encryption key from env (should be set in production)
        key_str = os.environ.get("TWILIO_ENCRYPTION_KEY")
        
        if key_str:
            # Use provided key (should be base64 encoded)
            try:
                return base64.urlsafe_b64decode(key_str)
            except:
                # If not base64, use it as seed
                key_str = key_str.encode()
        else:
            # Fallback: Generate deterministic key from master auth token
            # In production, set TWILIO_ENCRYPTION_KEY explicitly
            key_str = self.master_auth_token.encode() if self.master_auth_token else b"default_key_change_in_production"
        
        # Generate 32-byte key using SHA256
        key = hashlib.sha256(key_str).digest()
        return base64.urlsafe_b64encode(key)
    
    def encrypt_token(self, token: str) -> str:
        """
        Encrypt Twilio auth token using Fernet symmetric encryption.
        
        Args:
            token: Plain text auth token to encrypt
            
        Returns:
            Base64 encoded encrypted token
        """
        try:
            key = self._get_encryption_key()
            f = Fernet(key)
            encrypted = f.encrypt(token.encode())
            return base64.urlsafe_b64encode(encrypted).decode()
        except Exception as e:
            logger.error(f"[Twilio] Encryption error: {e}")
            raise ValueError(f"Failed to encrypt token: {e}")
    
    def decrypt_token(self, encrypted_token: str) -> str:
        """
        Decrypt Twilio auth token.
        
        Args:
            encrypted_token: Base64 encoded encrypted token
            
        Returns:
            Plain text auth token
        """
        try:
            key = self._get_encryption_key()
            f = Fernet(key)
            encrypted_bytes = base64.urlsafe_b64decode(encrypted_token.encode())
            decrypted = f.decrypt(encrypted_bytes)
            return decrypted.decode()
        except Exception as e:
            logger.error(f"[Twilio] Decryption error: {e}")
            raise ValueError(f"Failed to decrypt token: {e}")
    
    def create_subaccount(self, user_id: str) -> Dict[str, str]:
        """
        Create a Twilio subaccount for a user.
        
        Args:
            user_id: UUID string of the user
            
        Returns:
            Dictionary with 'subaccount_sid' and 'auth_token'
            
        Raises:
            ValueError: If credentials are missing or API call fails
            Exception: For other errors
        """
        if not self.master_sid or not self.master_auth_token:
            raise ValueError("Twilio master credentials not configured")
        
        friendly_name = f"user_{user_id}"
        url = f"{self.twilio_api_base}/Accounts.json"
        
        # Prepare Basic Auth
        auth = (self.master_sid, self.master_auth_token)
        
        # Request payload
        data = {
            "FriendlyName": friendly_name
        }
        
        try:
            logger.info(f"[Twilio] Creating subaccount for user {user_id}")
            response = requests.post(url, auth=auth, data=data, timeout=10)
            
            if response.status_code != 201:
                error_detail = response.text
                logger.error(f"[Twilio] API Error {response.status_code}: {error_detail}")
                raise ValueError(f"Twilio API error: {response.status_code} - {error_detail}")
            
            result = response.json()
            
            subaccount_sid = result.get("sid")
            auth_token = result.get("auth_token")
            
            if not subaccount_sid or not auth_token:
                logger.error(f"[Twilio] Invalid response from Twilio: {result}")
                raise ValueError("Invalid response from Twilio API")
            
            logger.info(f"[Twilio] Subaccount created: {subaccount_sid}")
            
            return {
                "subaccount_sid": subaccount_sid,
                "auth_token": auth_token
            }
            
        except requests.exceptions.RequestException as e:
            logger.error(f"[Twilio] Network error: {e}")
            raise Exception(f"Failed to connect to Twilio API: {e}")
        except Exception as e:
            logger.error(f"[Twilio] Unexpected error: {e}")
            raise


def create_twilio_subaccount(user_id: str, db_adapter) -> Tuple[bool, Optional[str]]:
    """
    Main function to create Twilio subaccount and store in database.
    
    Args:
        user_id: UUID string of the user
        db_adapter: SupabaseAdapter instance for database operations
        
    Returns:
        Tuple of (success: bool, error_message: Optional[str])
    """
    try:
        # Check if subaccount already exists
        existing = db_adapter.get_twilio_account(user_id)
        if existing:
            logger.info(f"[Twilio] Subaccount already exists for user {user_id}")
            return True, None
        
        # Initialize manager
        manager = TwilioSubaccountManager()
        
        # Create subaccount via Twilio API
        subaccount_data = manager.create_subaccount(user_id)
        
        # Encrypt auth token
        encrypted_token = manager.encrypt_token(subaccount_data["auth_token"])
        
        # Store in database
        success = db_adapter.save_twilio_account(
            user_id=user_id,
            subaccount_sid=subaccount_data["subaccount_sid"],
            encrypted_auth_token=encrypted_token,
            status="active"
        )
        
        if not success:
            error_msg = "Failed to save Twilio account to database"
            logger.error(f"[Twilio] {error_msg}")
            return False, error_msg
        
        logger.info(f"[Twilio] Successfully created and stored subaccount for user {user_id}")
        return True, None
        
    except ValueError as e:
        error_msg = str(e)
        logger.error(f"[Twilio] Validation error: {error_msg}")
        return False, error_msg
    except Exception as e:
        error_msg = f"Unexpected error creating Twilio subaccount: {str(e)}"
        logger.error(f"[Twilio] {error_msg}")
        import traceback
        traceback.print_exc()
        return False, error_msg
