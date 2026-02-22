import os
from dotenv import load_dotenv

# Ensure env vars are loaded if this module is imported directly
load_dotenv()

# Requires: pip install supabase
try:
    from supabase import create_client, Client
except ImportError:
    print("[WARN] 'supabase' module not found. Run: pip install supabase")
    Client = object # Dummy for typing

class SupabaseAdapter:
    def __init__(self):
        url: str = os.environ.get("SUPABASE_URL")
        # CRITICAL: Use Service Role Key for Backend to bypass RLS
        key: str = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_KEY")
        
        if not url or not key:
            print("[WARN] Supabase Credentials Missing. Database operations will fail.")
            self.client = None
        else:
            self.client: Client = create_client(url, key)

    # ========================================
    # USER PROFILES
    # ========================================
    
    def get_user_profile(self, user_id: str):
        """Fetch user profile by ID"""
        if not self.client: return None
        try:
            response = self.client.table("user_profiles").select("*").eq("user_id", user_id).single().execute()
            return response.data
        except Exception as e:
            print(f"[Supabase] Get Profile Error: {e}")
            return None
    
    def create_user_profile(self, user_id: str, display_name: str = None):
        """Create user profile (usually auto-created by trigger)"""
        if not self.client: return False
        try:
            self.client.table("user_profiles").insert({
                "user_id": user_id,
                "display_name": display_name
            }).execute()
            return True
        except Exception as e:
            # Profile might already exist, that's okay
            if "duplicate" in str(e).lower() or "unique" in str(e).lower():
                print(f"[Supabase] Profile already exists for {user_id}, continuing...")
                return True
            print(f"[Supabase] Create Profile Error: {e}")
            return False

    # ========================================
    # CALL LOGGING
    # ========================================
    
    def start_call(self, user_id: str, system_prompt: str = None, automation_metadata: str = None, connected_account: str = None):
        """Start a new call log"""
        if not self.client: return None
        try:
            data = {
                "user_id": user_id,
                "transcript": "",
                "status": "pending",
                "system_prompt": system_prompt,
                "automation_metadata": automation_metadata,
                "connected_account": connected_account
            }
            response = self.client.table("calls").insert(data).execute()
            if response.data:
                return response.data[0]['id']
        except Exception as e:
            print(f"[Supabase] Start Call Error: {e}")
        return None

    def log_call(self, user_id: str, transcript: str = "", status: str = "completed", system_prompt: str = None, metadata: str = None, connected_account: str = None, call_id: int = None):
        """Unified method to log a call (start or end). If call_id is provided, it updates the existing record."""
        if not self.client: return None
        try:
            data = {
                "user_id": user_id,
                "transcript": transcript,
                "status": status,
                "system_prompt": system_prompt,
                "automation_metadata": metadata,
                "connected_account": connected_account,
            }
            # Remove None values to avoid overwriting existing data with nulls in updates
            data = {k: v for k, v in data.items() if v is not None}
            
            if call_id:
                response = self.client.table("calls").update(data).eq("id", call_id).execute()
                print(f"[Supabase] Updated Call Record: {call_id}")
            else:
                data["created_at"] = "now()"
                response = self.client.table("calls").insert(data).execute()
                print(f"[Supabase] Created New Call Record")
                
            if response.data:
                return response.data[0]['id']
        except Exception as e:
            print(f"[Supabase] Log Call Error: {e}")
        return None

    def append_to_transcript(self, call_id: int, text_chunk: str):
        """Append text to call transcript"""
        if not self.client: return False
        try:
            # Fetch current transcript
            curr = self.client.table("calls").select("transcript").eq("id", call_id).single().execute()
            if curr.data:
                new_text = (curr.data['transcript'] or "") + text_chunk + "\n"
                self.client.table("calls").update({"transcript": new_text}).eq("id", call_id).execute()
                return True
        except Exception as e:
            print(f"[Supabase] Append Transcript Error: {e}")
        return False

    def end_call(self, call_id: int):
        """Mark call as completed"""
        if not self.client: return False
        try:
            self.client.table("calls").update({"status": "completed"}).eq("id", call_id).execute()
            return True
        except Exception as e:
            print(f"[Supabase] End Call Error: {e}")
            return False

    def update_call_analysis(self, call_id: int, status: str, result: str):
        """Update call with analysis results"""
        if not self.client: return False
        try:
            import json
            try:
                res_json = json.loads(result)
            except:
                res_json = {"raw": result}

            self.client.table("calls").update({
                "status": status, 
                "analysis_result": res_json
            }).eq("id", call_id).execute()
            return True
        except Exception as e:
            print(f"[Supabase] Update Analysis Error: {e}")
            return False
            
    def get_calls(self, user_id: str):
        """Get all calls for a user"""
        if not self.client: return []
        try:
            response = self.client.table("calls").select("*").eq("user_id", user_id).order("created_at", desc=True).execute()
            return response.data
        except Exception as e:
            print(f"[Supabase] Get Calls Error: {e}")
            return []
    
    def get_call_by_id(self, call_id: int):
        """Fetches a single call by ID"""
        if not self.client: return None
        try:
            response = self.client.table("calls").select("*").eq("id", call_id).single().execute()
            return response.data
        except Exception as e:
            print(f"[Supabase] Get Call By ID Error: {e}")
            return None

    # ========================================
    # GOOGLE TOKENS (via auth.identities)
    # ========================================
    
    def get_google_tokens(self, user_id: str):
        """
        Fetches Google Refresh Token from user_profiles.
        Used by Automation Engine to refresh access.
        """
        if not self.client: return None
        try:
            response = self.client.table("user_profiles").select("google_refresh_token").eq("user_id", user_id).single().execute()
            
            if response.data and response.data.get("google_refresh_token"):
                return {"refresh_token": response.data.get("google_refresh_token")}
                
            return None
        except Exception as e:
            # print(f"[Supabase] Get Google Tokens Error: {e}") 
            return None
            
    def store_google_refresh_token(self, user_id: str, refresh_token: str):
        """Store Google Refresh Token securely in user_profiles"""
        if not self.client: 
            print("[Supabase] Store Token Error: No Supabase client available")
            return False
        
        if not user_id:
            print("[Supabase] Store Token Error: user_id is None or empty")
            return False
            
        if not refresh_token:
            print("[Supabase] Store Token Error: refresh_token is None or empty")
            return False
        
        try:
            from datetime import datetime
            print(f"[Supabase] Attempting to store token for user_id: {user_id} (type: {type(user_id)})")
            
            # First, ensure the profile exists
            profile = self.get_user_profile(user_id)
            print(f"[Supabase] Profile exists: {profile is not None}")
            
            if not profile:
                # Create profile if it doesn't exist
                print(f"[Supabase] Creating profile for user_id: {user_id}")
                profile_created = self.create_user_profile(user_id)
                print(f"[Supabase] Profile creation result: {profile_created}")
            
            # UPSERT to ensure it works even if profile missing
            print(f"[Supabase] Attempting upsert with user_id: {user_id}, token length: {len(refresh_token)}")
            
            # Ensure user_id is a valid UUID string
            try:
                import uuid as uuid_lib
                # Validate and format as UUID string
                user_uuid = str(uuid_lib.UUID(str(user_id)))
            except ValueError as ve:
                print(f"[Supabase] Invalid UUID format for user_id: {user_id}, error: {ve}")
                return False
            
            upsert_data = {
                "user_id": user_uuid,
                "google_refresh_token": refresh_token,
                "updated_at": datetime.utcnow().isoformat()
            }
            print(f"[Supabase] Upsert data: user_id={user_uuid}, has_token={bool(refresh_token)}")
            
            result = self.client.table("user_profiles").upsert(
                upsert_data, 
                on_conflict="user_id"
            ).execute()
            
            print(f"[Supabase] Stored Google Refresh Token for {user_uuid}")
            if hasattr(result, 'data') and result.data:
                print(f"[Supabase] Upsert result: {result.data}")
            else:
                print(f"[Supabase] Upsert completed (no data returned)")
            return True
        except Exception as e:
             import traceback
             print(f"[Supabase] Store Token Error: {e}")
             print(f"[Supabase] Error Type: {type(e).__name__}")
             print(f"[Supabase] Traceback: {traceback.format_exc()}")
             return False

    def get_inventory(self):
        """Fetch all inventory (cars)"""
        if not self.client: return []
        try:
            return self.client.table("cars").select("*").execute().data
        except: return []
    
    def save_user_token(self, user_id: str, token_data: dict):
        """
        Saves Google OAuth tokens.
        Since we deprecated app_credentials, we store this as a 'google_workspace' integration
        in app_integrations table.
        """
        # DIRECT USE OF USER_ID (No Lookup Needed)
        
        return self.save_app_integration(
            user_id=user_id,
            service_type="google_workspace",
            config_json=token_data,
            is_active=True
        )

    # ========================================
    # APP OPTIONS
    # ========================================

    def get_app_options(self, user_id: str):
        """Fetch app configuration for a user"""
        if not self.client: return {}
        try:
            response = self.client.table("app_options").select("*").eq("user_id", user_id).single().execute()
            return response.data if response.data else {}
        except Exception as e:
            print(f"[Supabase] Get Options Error: {e}")
            return {}

    def save_app_options(self, user_id: str, options: dict) -> bool:
        """Save/update app options for a user"""
        if not self.client: return False
        try:
            # Add user_id and timestamp
            options['user_id'] = user_id
            options['updated_at'] = 'now()'
            
            # Upsert to app_options table
            self.client.table("app_options").upsert(options, on_conflict="user_id").execute()
            print(f"[Supabase] Saved app options for user {user_id}")
            return True
        except Exception as e:
            print(f"[Supabase] Save Options Error: {e}")
            return False

    def save_app_integration(self, user_id: str, service_type: str, config_json: dict, is_active: bool = True):
        """Save/update integration configuration"""
        if not self.client: return False
        try:
            config_json['user_id'] = user_id
            config_json['updated_at'] = 'now()'
            self.client.table("app_integrations").upsert(config_json).execute()
            return True
        except Exception as e:
            print(f"[Supabase] Save Integration Error: {e}")
            return False

    # ========================================
    # KNOWLEDGE BASE FUNCTIONS
    # ========================================

    def add_knowledge_item(self, user_id: str, title: str, content: str, 
                           category: str = 'general', tags: list = None, 
                           priority: int = 1) -> bool:
        """Add knowledge base item"""
        if not self.client: return False
        try:
            # Convert tags to JSON array
            tags_json = json.dumps(tags) if tags else "[]"

            result = self.client.table("knowledge_base").insert({
                "user_id": user_id,
                "title": title,
                "content": content,
                "category": category,
                "content_type": "text",
                "tags": tags_json,
                "priority": priority,
                "is_active": True
            }).execute()

            print(f"[Supabase] Added knowledge item: {title} for user {user_id}")
            return True
        except Exception as e:
            print(f"[Supabase] Add Knowledge Item Error: {e}")
            return False

    def get_knowledge_items(self, user_id: str, category: str = None, 
                          limit: int = 50) -> list:
        """Get knowledge base items with optional category filter"""
        if not self.client: return []
        try:
            query = self.client.table("knowledge_base").select("*").eq("user_id", user_id).eq("is_active", True)

            if category:
                query = query.eq("category", category)

            query = query.order("priority", desc=True).limit(limit)
            response = query.execute()

            items = []
            if response.data:
                for item in response.data:
                    # Parse tags back to list
                    tags_list = json.loads(item.get("tags", "[]"))
                    items.append({
                        "id": item.get("id"),
                        "title": item.get("title"),
                        "content": item.get("content"),
                        "category": item.get("category"),
                        "tags": tags_list,
                        "priority": item.get("priority"),
                        "created_at": item.get("created_at")
                    })

            return items
        except Exception as e:
            print(f"[Supabase] Get Knowledge Items Error: {e}")
            return []

    def update_knowledge_item(self, user_id: str, item_id: str, **kwargs) -> bool:
        """Update knowledge base item"""
        if not self.client: return False
        try:
            # Prepare update data
            update_data = {}
            allowed_fields = ['title', 'content', 'category', 'tags', 'priority', 'is_active']

            for key, value in kwargs.items():
                if key in allowed_fields:
                    if key == 'tags':
                        update_data[key] = json.dumps(value) if isinstance(value, list) else value
                    else:
                        update_data[key] = value

            result = self.client.table("knowledge_base").update(update_data).eq("id", item_id).eq("user_id", user_id).execute()

            print(f"[Supabase] Updated knowledge item: {item_id}")
            return True
        except Exception as e:
            print(f"[Supabase] Update Knowledge Item Error: {e}")
            return False

    def delete_knowledge_item(self, user_id: str, item_id: str) -> bool:
        """Delete knowledge base item"""
        if not self.client: return False
        try:
            result = self.client.table("knowledge_base").delete().eq("id", item_id).eq("user_id", user_id).execute()

            print(f"[Supabase] Deleted knowledge item: {item_id}")
            return True
        except Exception as e:
            print(f"[Supabase] Delete Knowledge Item Error: {e}")
            return False

    # ========================================
    # APP INTEGRATIONS
    # ========================================

    def get_app_integrations(self, user_id: str):
        """Fetch all integrations for a user"""
        if not self.client: return []
        try:
            response = self.client.table("app_integrations").select("*").eq("user_id", user_id).execute()
            return response.data
        except Exception as e:
            print(f"[Supabase] Get Integrations Error: {e}")
            return []

    def save_app_integration(self, user_id: str, service_type: str, config_json: dict, is_active: bool = True):
        """Save/update integration configuration"""
        if not self.client: return False
        try:
            data = {
                "user_id": user_id,
                "service_type": service_type,
                "config_json": config_json,
                "is_active": is_active,
                "updated_at": "now()"
            }
            self.client.table("app_integrations").upsert(data).execute()
            return True
        except Exception as e:
            print(f"[Supabase] Save Integration Error: {e}")
            return False

    def delete_app_integration(self, user_id: str, integration_id: str):
        """Delete an integration"""
        if not self.client: return False
        try:
            self.client.table("app_integrations").delete().eq("id", integration_id).eq("user_id", user_id).execute()
            return True
        except Exception as e:
            print(f"[Supabase] Delete Integration Error: {e}")
            return False

    # ========================================
    # WORKFLOWS (NEW)
    # ========================================

    def get_workflows(self, user_id: str):
        """Fetch all workflows for a user"""
        if not self.client: return []
        try:
            response = self.client.table("app_workflows").select("*").eq("user_id", user_id).order("updated_at", desc=True).execute()
            return response.data
        except Exception as e:
            print(f"[Supabase] Get Workflows Error: {e}")
            return []

    def get_workflow_by_id(self, workflow_id: str):
        """Fetch single workflow"""
        if not self.client: return None
        try:
            response = self.client.table("app_workflows").select("*").eq("id", workflow_id).execute()
            if response.data:
                return response.data[0]
            return None
        except Exception as e:
            print(f"[Supabase] Get Workflow Error: {e}")
            return None

    def save_workflow(self, user_id: str, workflow_data: dict):
        """Save/Upsert workflow"""
        if not self.client: return False
        try:
            # Ensure user_id is set
            workflow_data["user_id"] = user_id
            workflow_data["updated_at"] = "now()"
            
            # Remove keys that shouldn't be here if any
            if "created_at" in workflow_data:
                del workflow_data["created_at"]

            response = self.client.table("app_workflows").upsert(workflow_data).execute()
            return response.data[0] if response.data else True
        except Exception as e:
            print(f"[Supabase] Save Workflow Error: {e}")
            return False

    def delete_workflow(self, user_id: str, workflow_id: str):
        """Delete a workflow"""
        if not self.client: return False
        try:
            self.client.table("app_workflows").delete().eq("id", workflow_id).eq("user_id", user_id).execute()
            return True
        except Exception as e:
            print(f"[Supabase] Delete Workflow Error: {e}")
            return False

    def get_active_workflow_for_trigger(self, user_id: str, trigger_type: str = 'call_ended', run_on: str = 'any'):
        """
        Fetch the active workflow for a specific trigger context.
        run_on: 'inbound', 'outbound', or 'any'
        """
        if not self.client: return None
        try:
            # Query active workflows for this user and trigger type
            query = self.client.table("app_workflows").select("*") \
                .eq("user_id", user_id) \
                .eq("is_active", True) \
                .eq("trigger_type", trigger_type)
            
            response = query.execute()
            workflows = response.data
            
            if not workflows:
                return None
                
            # Filter matches
            matches = []
            for wf in workflows:
                config = wf.get('trigger_config', {})
                wf_run_on = config.get('run_on', 'any')
                
                # Logic: 
                # If we need 'inbound', we accept 'inbound' or 'any'
                # If we need 'outbound', we accept 'outbound' or 'any'
                if wf_run_on == run_on or wf_run_on == 'any':
                     matches.append(wf)
            
            # Sort by updated_at desc (most recent first)
            matches.sort(key=lambda x: x.get('updated_at', ''), reverse=True)
            
            return matches[0] if matches else None
            
        except Exception as e:
            print(f"[Supabase] Get Active Workflow Error: {e}")
            return None

    # ========================================
    # ADMIN HELPERS
    # ========================================

    def get_leads(self, user_id: str):
        """Fetch leads for user"""
        if not self.client: return []
        try:
            return self.client.table("leads").select("*").eq("user_id", user_id).order("created_at", desc=True).execute().data
        except Exception as e:
            print(f"[Supabase] Get Leads Error: {e}")
            return []

    def update_call_status(self, call_id, status):
        """Update status of a call"""
        if not self.client: return False
        try:
            self.client.table("calls").update({"status": status}).eq("id", call_id).execute()
            return True
        except Exception as e:
             print(f"[Supabase] Update Status Error: {e}")
             return False

    def decode_jwt(self, token):
        """Decode JWT (Unverified extraction)"""
        try:
            import jwt
            return jwt.decode(token, options={"verify_signature": False})
        except Exception as e:
            print(f"[Supabase] Decode JWT Error: {e}")
            return {}

    def get_user_settings(self, user_id: str):
        """Fetch all user settings (options + integrations)"""
        if not self.client: return {}
        settings = {}
        try:
            # 1. Fetch App Options
            options = self.client.table("app_options").select("*").eq("user_id", user_id).single().execute()
            if options.data:
                settings['calendar_enabled'] = options.data.get('enable_calendar', False)
                settings['automation_rules'] = options.data.get('automation_rules', [])
                settings['timezone'] = options.data.get('timezone', 'UTC')
            
            # 2. Fetch Google Refresh Token from User Profile
            profile = self.client.table("user_profiles").select("google_refresh_token").eq("user_id", user_id).single().execute()
            if profile.data and profile.data.get('google_refresh_token'):
                # Structure it as GroqAgent expects
                settings['google_tokens'] = {'refresh_token': profile.data['google_refresh_token']}

            # 3. Fetch Phone from Twilio Integration (if exists)
            integrations = self.client.table("app_integrations").select("*").eq("user_id", user_id).eq("service_type", "twilio").execute()
            if integrations.data:
                # Assuming first active twilio config has phone
                for integ in integrations.data:
                    if integ.get('is_active'):
                        config = integ.get('config_json', {})
                        if config.get('phone_number'):
                            settings['phone_number'] = config.get('phone_number')
                            break
                            
        except Exception as e:
            print(f"[Supabase] Get Settings Error: {e}")
        
        return settings

    # ========================================
    # ADMIN LOOKUP
    
    def get_user_id_by_email(self, email: str):
        """Lookup user ID from email using admin client"""
        service_role_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        url = os.environ.get("SUPABASE_URL")
        
        if not service_role_key or not url:
            return None
            
        try:
            admin_client = create_client(url, service_role_key)
            user_response = admin_client.auth.admin.list_users()
            
            for u in user_response:
                if u.email == email:
                    return u.id
        except Exception as e:
            print(f"[Supabase Admin] Lookup Error: {e}")
        return None

    # ========================================
    # TWILIO ACCOUNTS
    
    def get_twilio_account(self, user_id: str):
        """Get Twilio account for a user"""
        if not self.client:
            return None
        try:
            response = self.client.table("twilio_accounts").select("*").eq("user_id", user_id).single().execute()
            return response.data
        except Exception as e:
            # print(f"[Supabase] Get Twilio Account Error: {e}")
            return None

    def get_user_by_subaccount_sid(self, sid: str):
        """Find user_id by Twilio Subaccount SID"""
        if not self.client: return None
        try:
            response = self.client.table("twilio_accounts").select("user_id").eq("subaccount_sid", sid).single().execute()
            if response.data:
                return response.data.get('user_id')
            return None
        except Exception as e:
            print(f"[Supabase] Get User By SID Error: {e}")
            return None
        try:
            response = self.client.table("twilio_accounts").select("*").eq("user_id", user_id).single().execute()
            return response.data if response.data else None
        except Exception as e:
            # Not found is okay
            return None
    
    def save_twilio_account(self, user_id: str, subaccount_sid: str, encrypted_auth_token: str, status: str = "active") -> bool:
        """Save or update Twilio account information"""
        if not self.client:
            print("[Supabase] Save Twilio Account Error: No Supabase client available")
            return False
        
        try:
            from datetime import datetime
            import uuid as uuid_lib
            
            # Ensure user_id is valid UUID string
            user_uuid = str(uuid_lib.UUID(str(user_id)))
            
            result = self.client.table("twilio_accounts").upsert({
                "user_id": user_uuid,
                "subaccount_sid": subaccount_sid,
                "encrypted_auth_token": encrypted_auth_token,
                "status": status,
                "updated_at": datetime.utcnow().isoformat()
            }, on_conflict="user_id").execute()
            
            print(f"[Supabase] Saved Twilio account for user {user_uuid}")
            return True
        except Exception as e:
            import traceback
            print(f"[Supabase] Save Twilio Account Error: {e}")
            print(f"[Supabase] Traceback: {traceback.format_exc()}")
            return False
    
    def check_is_outbound_call(self, call_sid: str) -> bool:
        """Check if a call SID exists in the outbound_calls table"""
        if not self.client or not call_sid: return False
        try:
            response = self.client.table("outbound_calls").select("id").eq("call_sid", call_sid).execute()
            return bool(response.data and len(response.data) > 0)
        except Exception as e:
            print(f"[Supabase] Check Outbound Error: {e}")
            return False

    def get_twilio_credentials(self, user_id: str) -> dict:
        """
        Get decrypted Twilio credentials for a user.
        Returns dict with 'subaccount_sid' and 'auth_token' (decrypted).
        """
        account = self.get_twilio_account(user_id)
        if not account:
            return None
        
        try:
            from backend.twilio_subaccount import TwilioSubaccountManager
            manager = TwilioSubaccountManager()
            decrypted_token = manager.decrypt_token(account["encrypted_auth_token"])
            
            return {
                "subaccount_sid": account["subaccount_sid"],
                "auth_token": decrypted_token,
                "status": account.get("status", "active")
            }
        except Exception as e:
            print(f"[Supabase] Error decrypting Twilio token: {e}")
            return None

    # ========================================
    # TWILIO SUBACCOUNT MANAGEMENT
    # ========================================
    
    def save_twilio_account(self, user_id: str, subaccount_data: dict) -> bool:
        """
        Save Twilio subaccount credentials for a user.
        
        Args:
            user_id: User UUID
            subaccount_data: Dict with keys:
                - subaccount_sid
                - encrypted_auth_token
                - friendly_name
                - status (optional, defaults to 'active')
        
        Returns:
            True if successful, False otherwise
        """
        if not self.client:
            print("[Supabase] Client not initialized")
            return False
        
        try:
            data = {
                "user_id": user_id,
                "subaccount_sid": subaccount_data["subaccount_sid"],
                "encrypted_auth_token": subaccount_data["encrypted_auth_token"],
                "friendly_name": subaccount_data["friendly_name"],
                "status": subaccount_data.get("status", "active")
            }
            
            # Use upsert to handle duplicates (shouldn't happen, but safety first)
            response = self.client.table("twilio_accounts").upsert(
                data,
                on_conflict="user_id"
            ).execute()
            
            if response.data:
                print(f"[Supabase] Twilio account saved for user {user_id}")
                return True
            else:
                print(f"[Supabase] Failed to save Twilio account (no data returned)")
                return False
                
        except Exception as e:
            print(f"[Supabase] Error saving Twilio account: {e}")
            return False
    
    def get_twilio_account(self, user_id: str) -> dict:
        """
        Retrieve Twilio subaccount credentials for a user.
        
        Args:
            user_id: User UUID
        
        Returns:
            Dictionary with subaccount details or None if not found
        """
        if not self.client:
            return None
        
        try:
            response = self.client.table("twilio_accounts").select("*").eq("user_id", user_id).single().execute()
            
            if response.data:
                return {
                    "subaccount_sid": response.data["subaccount_sid"],
                    "encrypted_auth_token": response.data["encrypted_auth_token"],
                    "friendly_name": response.data["friendly_name"],
                    "status": response.data["status"],
                    "created_at": response.data.get("created_at")
                }
            return None
            
        except Exception as e:
            # Not found is expected for new users
            if "PGRST116" in str(e):  # Postgrest "no rows" error
                return None
            print(f"[Supabase] Error fetching Twilio account: {e}")
            return None
    
    def update_twilio_account_status(self, user_id: str, status: str) -> bool:
        """
        Update the status of a Twilio subaccount.
        
        Args:
            user_id: User UUID
            status: New status ('active', 'suspended', 'deleted')
        
        Returns:
            True if successful
        """
        if not self.client:
            return False
        
        if status not in ['active', 'suspended', 'deleted']:
            print(f"[Supabase] Invalid status: {status}")
            return False
        
        try:
            response = self.client.table("twilio_accounts").update({
                "status": status
            }).eq("user_id", user_id).execute()
            
            if response.data:
                print(f"[Supabase] Twilio account status updated to '{status}' for user {user_id}")
                return True
            return False
            
        except Exception as e:
            print(f"[Supabase] Error updating Twilio account status: {e}")
            return False



# Global instance
# Global instance
supabase_adapter = SupabaseAdapter()
# Legacy alias for server compatibility
supabase = supabase_adapter.client
