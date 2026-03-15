import os
import sys
from typing import Optional

# Windows Unicode Fix
sys.stdout.reconfigure(encoding='utf-8')

from dotenv import load_dotenv
# Load env variables FIRST
load_dotenv()

# Global Constant to avoid UnboundLocalError in functions
GROQ_KEY = os.environ.get("GROQ_API_KEY")
if not GROQ_KEY:
    print("[CRITICAL] GROQ_API_KEY missing from environment. Agent will fail.")
import uvicorn
import asyncio
import json
import base64
import numpy as np
import io
import wave
import time
import automation_engine  # NEW IMPORT - Now Safe
import requests
import urllib.parse
import traceback
import hmac
import hashlib

# ── Audio helpers (replaces deprecated audioop) ──────────────────────────────
# audioop was removed in Python 3.13. These numpy functions do the same thing.

def _ulaw2lin(ulaw_bytes: bytes) -> bytes:
    """μ-law → 16-bit PCM. Equivalent to audioop.ulaw2lin(data, 2)."""
    u = np.frombuffer(ulaw_bytes, dtype=np.uint8).astype(np.int16)
    u = ~u
    sign = (u & 0x80)
    exp  = (u >> 4) & 0x07
    mant = u & 0x0F
    lin  = (mant << (exp + 3)) + (33 << exp) - 33
    lin  = np.where(sign != 0, -lin, lin)
    return lin.astype(np.int16).tobytes()

def _lin2ulaw(pcm_bytes: bytes) -> bytes:
    """16-bit PCM → μ-law. Equivalent to audioop.lin2ulaw(data, 2)."""
    s = np.frombuffer(pcm_bytes, dtype=np.int16).astype(np.int32)
    sign = np.where(s < 0, np.uint8(0x80), np.uint8(0)).astype(np.uint8)
    s = np.minimum(np.abs(s), 32767) + 33
    exp_lut = np.array([0,1,2,2,3,3,3,3,4,4,4,4,4,4,4,4,
                        5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,
                        6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,
                        6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,
                        7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,
                        7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,
                        7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,
                        7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7], dtype=np.uint8)
    idx  = np.minimum(s >> 7, 127).astype(np.uint8)
    exp  = exp_lut[idx]
    mant = ((s >> (exp.astype(np.int32) + 3)) & 0x0F).astype(np.uint8)
    ulaw = (~(sign | (exp << 4) | mant)).astype(np.uint8)
    return ulaw.tobytes()

def _ratecv(pcm_bytes: bytes, in_rate: int, out_rate: int) -> bytes:
    """Resample 16-bit mono PCM. Equivalent to audioop.ratecv(data,2,1,in_rate,out_rate,None)."""
    if in_rate == out_rate or not pcm_bytes:
        return pcm_bytes
    samples = np.frombuffer(pcm_bytes, dtype=np.int16).astype(np.float32)
    n_out   = int(len(samples) * out_rate / in_rate)
    resampled = np.interp(
        np.linspace(0, len(samples) - 1, n_out),
        np.arange(len(samples)),
        samples
    ).astype(np.int16)
    return resampled.tobytes()

def _rms(pcm_bytes: bytes) -> int:
    """RMS energy of 16-bit PCM. Equivalent to audioop.rms(data, 2)."""
    if not pcm_bytes:
        return 0
    samples = np.frombuffer(pcm_bytes, dtype=np.int16).astype(np.float32)
    return int(np.sqrt(np.mean(samples ** 2)))
# ─────────────────────────────────────────────────────────────────────────────

from fastapi import FastAPI, WebSocket, Request, Query, WebSocketDisconnect, UploadFile, File, Depends, Response, HTTPException, APIRouter
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from agent_groq import GroqAgent
# from database_adapter import DBAdapter (Removed)
import shutil

# Import knowledge base API
from csv_import_api import router as knowledge_router

# Import Twilio Provisioning Service
# Import Twilio Provisioning Service
from twilio_provisioning import get_provisioner
from backend.supabase_client import supabase # Direct DB Access

app = FastAPI()

# CORS — allow all origins so local UI and frontend can call the API
from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Rate Limiting ───────────────────────────────────────────────────────────────────────
# Stop a single bad actor from burning your Twilio/Groq credits.
# Limit: 20 call-initiation requests per minute per IP.
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

_limiter = Limiter(key_func=get_remote_address, default_limits=["200/minute"])
app.state.limiter = _limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
# ────────────────────────────────────────────────────────────────────────

# ── Twilio Webhook Validator ──────────────────────────────────────────────
# Twilio signs every webhook request. We verify the signature so only
# real Twilio requests can trigger our call handling logic.
# In DEBUG_MODE the check is skipped so local testing works without signing.
_DEBUG_MODE = os.environ.get("DEBUG_MODE", "false").lower() == "true"
_TWILIO_AUTH_TOKEN = os.environ.get("TWILIO_MASTER_AUTH_TOKEN", "")

def _validate_twilio_request(request: Request, form_data: dict) -> bool:
    """Returns True if the request is a legitimate Twilio webhook."""
    if _DEBUG_MODE:
        return True  # Skip in local dev
    try:
        from twilio.request_validator import RequestValidator
        validator = RequestValidator(_TWILIO_AUTH_TOKEN)
        signature = request.headers.get("X-Twilio-Signature", "")
        url = str(request.url)
        return validator.validate(url, form_data, signature)
    except Exception:
        return False
# ────────────────────────────────────────────────────────────────────────

app.include_router(knowledge_router)

# Call management router (list numbers, make/hangup calls, history, webhooks)
from routes.calls import router as calls_router
app.include_router(calls_router)

# Workflow CRUD router (deduplicated from 3x definitions)
from routes.workflows import router as workflows_router
app.include_router(workflows_router)

# Data endpoints (leads, inventory, knowledge base)
from routes.data import router as data_router
app.include_router(data_router)

# ── Health Check Endpoints ────────────────────────────────────────────────────
# Used by load balancers and deployment platforms to check if the server is alive.
@app.get("/health")
async def health_check():
    return {"status": "ok"}

@app.get("/ready")
async def readiness_check():
    return {"status": "ready"}
# ──────────────────────────────────────────────────────────────────────────────

# Serve static UI files
import os as _os
_static_dir = _os.path.join(_os.path.dirname(__file__), "static")
if _os.path.isdir(_static_dir):
    app.mount("/static", StaticFiles(directory=_static_dir), name="static")

@app.get("/", response_class=HTMLResponse)
async def serve_landing():
    ui_path = _os.path.join(_static_dir, "index.html")
    if _os.path.exists(ui_path):
        with open(ui_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>Landing page not found</h1>", status_code=404)

@app.get("/console", response_class=HTMLResponse)
async def serve_console():
    console_path = _os.path.join(_static_dir, "console.html")
    if _os.path.exists(console_path):
        with open(console_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>Console UI not found</h1>", status_code=404)

# --- SUPABASE AUTH SETUP ---
from backend.supabase_client import supabase, supabase_adapter  # GLOBAL IMPORT
from fastapi import HTTPException, status, Header
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from dependencies import get_current_user, security

# Global user session storage for Twilio integration
ACTIVE_USER_SESSIONS = {}  # {session_id: {user_id, user_email, settings}}

async def fetch_twilio_call_details(call_sid: str, account_sid: str = None):
    """
    Fetches call details (From/To) directly from Twilio REST API.
    Used as a fallback when WebSocket parameters are missing.
    """
    master_sid = os.environ.get("TWILIO_MASTER_SID")
    master_token = os.environ.get("TWILIO_MASTER_AUTH_TOKEN")
    
    if not master_sid or not master_token or not call_sid:
        return None
        
    # If account_sid not provided, use master_sid
    effective_account_sid = account_sid or master_sid
    
    url = f"https://api.twilio.com/2010-04-01/Accounts/{effective_account_sid}/Calls/{call_sid}.json"
    
    import httpx
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, auth=(master_sid, master_token))
            print(f"[TWILIO API] Response Code: {resp.status_code}")
            if resp.status_code == 200:
                data = resp.json()
                print(f"[TWILIO API] Data: {data}") # DEBUG
                return {
                    "from": data.get("from") or data.get("From"),
                    "to": data.get("to") or data.get("To"),
                    "direction": data.get("direction")
                }
            else:
                print(f"[TWILIO API] Error {resp.status_code}: {resp.text}")
    except Exception as e:
        print(f"[TWILIO API] Exception fetching call details: {e}")
    
    return None

async def extract_phone_with_fallback(custom_params, call_sid, account_sid, call_type='inbound'):
    """Unified logic to extract phone number + REST API fallback."""
    phone = (
        custom_params.get('phone_number') or 
        custom_params.get('call_from') or 
        custom_params.get('From') or 
        custom_params.get('phone') or
        custom_params.get('To') or
        custom_params.get('caller') or
        'Unknown'
    )
    
    if phone == "Unknown" and call_sid:
        print(f"[DEBUG] Phone Unknown. Attempting REST API Fallback for {call_sid}")
        details = await fetch_twilio_call_details(call_sid, account_sid)
        if details:
            if call_type == 'outbound':
                phone = details.get('to') or 'Unknown'
            else:
                phone = details.get('from') or 'Unknown'
            print(f"[TWILIO API] 🎯 Successfully recovered phone: {phone}")
        else:
            print(f"[TWILIO API] ❌ Failed to recover phone via API.")
            
    return phone

def perform_safe_logging(u_id, c_type, c_sid, phone, prompt_text="N/A", status="in-progress", account="web-direct"):
    """Global helper to log call starts with metadata."""
    try:
        m_payload = {
            "call_type": c_type,
            "call_sid": c_sid,
            "phone_number": phone
        }
        m_json = json.dumps(m_payload)
        print(f"[LOGGING] Metadata Payload: {m_json}")
        
        return supabase_adapter.log_call(
                user_id=u_id,
                transcript="[Call Started]",
                status=status,
                connected_account=account,
                system_prompt=prompt_text,
                metadata=m_json
        )
    except Exception as e:
        print(f"[LOGGING] ERROR: {e}")
        return None

async def get_current_user_ws(token: str = Query(None)):
    """Verifies token for WebSocket connection"""
    if not token:
        # Fallback for dev/testing if needed, or reject
        print("WS Connection missing token")
        return None
        # raise WebSocketDisconnect(code=4001, reason="Missing Auth Token")
    
    # Handle OAuth success token (temporary solution)
    if token == "oauth_success_token":
        print("[WS DEBUG] OAuth Success Token Detected")
        # Create a dummy user object for OAuth users
        class DummyUser:
            def __init__(self):
                self.id = "655b1b48-66b6-4455-9a92-3fcac8c377eb"  # Known user ID from logs
                self.email = "rahulsamineni1234@gmail.com"
                self.aud = "authenticated"
                self.role = "authenticated"
                self.app_metadata = {}
                self.user_metadata = {"provider": "google"}
                self.created_at = "2025-01-01T00:00:00Z"
        
        return DummyUser()
    
    try:
        user = supabase.auth.get_user(token)
        return user.user
    except Exception as e:
        print(f"WS Auth Error: {e}")
        return None

    return {}

class GoogleToken(BaseModel):
    provider_token: str
    provider_refresh_token: str | None = None
    expires_in: int | None = None
    token_type: str | None = None

@app.post("/api/auth/sync_google")
async def sync_google_token(token_data: GoogleToken, user: dict = Depends(get_current_user)):
    """
    Receives Google Tokens from Frontend and securely stores them in 'app_credentials'.
    This is the 'Standard' way to persist keys when using Client-Side Login.
    """
    from backend.supabase_client import supabase_adapter
    
    # DEPRECATED: save_app_credential. Use direct refresh token storage.
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

# --- TWILIO SUBACCOUNT AUTO-PROVISIONING ---

class TwilioProvisionRequest(BaseModel):
    email: Optional[str] = None  # Optional email, will use user.email from JWT if not provided

@app.post("/api/auth/provision-twilio")
async def provision_twilio_subaccount(data: TwilioProvisionRequest, user: dict = Depends(get_current_user)):
    """
    Provision a Twilio subaccount for the authenticated user.
    User ID is automatically extracted from the JWT token.
    """
    print(f"[TWILIO PROVISION] ======== START ========")
    print(f"[TWILIO PROVISION] User ID: {user.id}")
    print(f"[TWILIO PROVISION] User Email: {user.email}")
    print(f"[TWILIO PROVISION] Request Email: {data.email}")
    
    from backend.supabase_client import supabase_adapter
    
    # Check if subaccount already exists
    print(f"[TWILIO PROVISION] Checking for existing subaccount...")
    try:
        existing = supabase_adapter.get_twilio_account(user.id)
        print(f"[TWILIO PROVISION] Existing account check result: {existing}")
    except Exception as e:
        print(f"[TWILIO PROVISION ERROR] Failed to check existing account: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Database error: {str(e)}"
        )
    
    if existing:
        print(f"[TWILIO] Subaccount already exists for user {user.id}")
        return {
            "status": "exists",
            "message": "Twilio subaccount already provisioned",
            "subaccount_sid": existing["subaccount_sid"],
            "friendly_name": existing["friendly_name"]
        }
    
    try:
        # Create Twilio subaccount
        print(f"[TWILIO PROVISION] Creating new subaccount...")
        provisioner = get_provisioner()
        print(f"[TWILIO PROVISION] Provisioner initialized")
        
        subaccount_data = provisioner.create_subaccount(
            user_id=user.id,
            email=data.email or user.email
        )
        print(f"[TWILIO PROVISION] Subaccount created: {subaccount_data.get('subaccount_sid')}")
        
        # Save to database
        print(f"[TWILIO PROVISION] Saving to database...")
        success = supabase_adapter.save_twilio_account(
            user_id=user.id,
            subaccount_data=subaccount_data
        )
        print(f"[TWILIO PROVISION] Save result: {success}")
        
        if not success:
            raise HTTPException(
                status_code=500,
                detail="Failed to save Twilio account to database"
            )
        
        print(f"[TWILIO PROVISION] ======== SUCCESS ========")
        return {
            "status": "success",
            "message": "Twilio subaccount provisioned successfully",
            "subaccount_sid": subaccount_data["subaccount_sid"],
            "friendly_name": subaccount_data["friendly_name"]
       }
        
    except Exception as e:
        print(f"[TWILIO PROVISION ERROR] ======== FAILED ========")
        print(f"[TWILIO PROVISION ERROR] Exception type: {type(e).__name__}")
        print(f"[TWILIO PROVISION ERROR] Exception message: {str(e)}")
        import traceback
        print(f"[TWILIO PROVISION ERROR] Traceback:\n{traceback.format_exc()}")
        raise HTTPException(
            status_code=500,
            detail=f"Twilio provisioning failed: {str(e)}"
        )

# Get Twilio account status
@app.get("/api/twilio/status")
async def get_twilio_status(user: dict = Depends(get_current_user)):
    """
    Retrieve Twilio subaccount status for the current user.
    """
    from backend.supabase_client import supabase_adapter
    
    account = supabase_adapter.get_twilio_account(user.id)
    
    if not account:
        return {
            "status": "not_provisioned",
            "message": "No Twilio subaccount found"
        }
    
    return {
        "status": account["status"],
        "subaccount_sid": account["subaccount_sid"],
        "friendly_name": account["friendly_name"],
        "created_at": account["created_at"]
    }

# --- OUTBOUND CALLS API ENDPOINTS ---


# --- CALL ENDPOINTS MOVED TO routes/calls.py ---



# --- INTEGRATION CONFIGURATION ---

# LEGACY INTEGRATION ENDPOINTS REMOVED


# --- USER PROFILE ---

@app.get("/api/user/profile")
async def get_user_profile(user: dict = Depends(get_current_user)):
    """
    Returns full user profile from Supabase including full_name, email, and avatar_url
    """
    try:
        email = user.email
        
        # Get user info from Supabase
        user_response = supabase.auth.admin.list_users()
        
        # Find the current user in the list
        for supabase_user in user_response:
            if supabase_user.email == email:
                user_meta = supabase_user.user_metadata or {}
                return {
                    "email": supabase_user.email,
                    "full_name": user_meta.get("full_name") or user_meta.get("name") or "User",
                    "avatar_url": user_meta.get("avatar_url") or user_meta.get("picture")
                }
        
        # Fallback if user not found in admin list
        return {"email": email, "full_name": "User", "avatar_url": None}
    except Exception as e:
        print(f"[ERROR] Profile fetch error: {e}")
        return {"email": user.email, "full_name": "User", "avatar_url": None}

# --- GOOGLE INTEGRATION (Standardized) ---

@app.get("/api/sheets")
async def list_sheets(user: dict = Depends(get_current_user)):
    """Fetches Google Sheets using credentials from the Vault"""
    
    # 1. Get Token from Vault
    from backend.supabase_client import supabase_adapter
    token_data = supabase_adapter.get_google_tokens(user.id)
    
    if not token_data or not token_data.get("refresh_token"):
        return {"files": [], "error": "Google Account not linked. Please re-login."}
        
    try:
        # 2. Build Credentials Object
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
        
        creds = Credentials(
            token=None, # Access token not stored, auto-refresh will happen
            refresh_token=token_data.get("refresh_token"),
            token_uri="https://oauth2.googleapis.com/token",
            client_id=os.environ.get("GOOGLE_CLIENT_ID"), # Optional
            client_secret=os.environ.get("GOOGLE_CLIENT_SECRET"),
            scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.readonly"]
        )
        
        # 3. Call Drive API
        service = build('drive', 'v3', credentials=creds)
        results = service.files().list(
            q="mimeType='application/vnd.google-apps.spreadsheet'",
            pageSize=10, fields="nextPageToken, files(id, name)"
        ).execute()
        
        items = results.get('files', [])
        return {"files": [{"id": f["id"], "name": f["name"]} for f in items]}
        
    except Exception as e:
        print(f"[GOOGLE] Error listing sheets: {e}")
        return {"files": [], "error": str(e)}

@app.get("/api/calendars")
async def list_calendars(user: dict = Depends(get_current_user)):
    return {"status": "ok", "message": "Calendar integration ready"}

@app.post("/api/sheets/select")
async def select_sheet(payload: dict):
    return {"status": "success", "message": "Sheet selected"}

@app.get("/api/sheets/{sheet_id}/data")
async def get_sheet_data(sheet_id: str, user: dict = Depends(get_current_user)):
    """
    Fetch data from a specific Google Sheet for column selection and import.
    """
    from backend.supabase_client import supabase_adapter
    token_data = supabase_adapter.get_google_tokens(user.id)
    
    if not token_data or not token_data.get("refresh_token"):
        raise HTTPException(status_code=401, detail="Google Account not linked")
    
    try:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
        
        creds = Credentials(
            token=None,
            refresh_token=token_data.get("refresh_token"),
            token_uri="https://oauth2.googleapis.com/token",
            client_id=os.environ.get("GOOGLE_CLIENT_ID"),
            client_secret=os.environ.get("GOOGLE_CLIENT_SECRET"),
            scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.readonly"]
        )
        
        service = build('sheets', 'v4', credentials=creds)
        
        # Get sheet data
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range='A1:ZZ1000'  # Get first 1000 rows, all columns
        ).execute()
        
        values = result.get('values', [])
        
        if not values:
            return {"rows": []}
        
        # Convert to list of dictionaries using first row as headers
        headers = values[0]
        rows = []
        
        for row_data in values[1:]:
            # Pad row if it's shorter than headers
            row_dict = {}
            for i, header in enumerate(headers):
                row_dict[header] = row_data[i] if i < len(row_data) else ""
            rows.append(row_dict)
        
        return {"rows": rows, "headers": headers}
        
    except Exception as e:
        print(f"[SHEETS] Error fetching sheet data: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch sheet data: {str(e)}")

@app.post("/api/calendars/select")
async def select_calendar(payload: dict):
    return {"status": "error", "message": "Disabled"}


from fastapi.responses import RedirectResponse


# --- TWILIO INTEGRATION ---
@app.post("/twilio/incoming")
async def twilio_incoming(request: Request):
    print(f"[TWILIO INCOMING] Hit received! Params: {request.query_params}")
    # Verify this request is actually from Twilio (skip in DEBUG_MODE)
    form_data = dict(await request.form())
    if not _validate_twilio_request(request, form_data):
        raise HTTPException(status_code=403, detail="Invalid Twilio signature")
    """
    Handle incoming calls from Twilio.
    Returns TwiML to connect to the Media Stream.
    """
    # WebSocket URL — prefer BASE_URL env var, fallback to request host
    _base = os.environ.get("BASE_URL", "").rstrip("/")
    if _base:
        ws_url = _base.replace("https://", "wss://").replace("http://", "ws://") + "/twilio/ws"
    else:
        host = request.headers.get("host", "tinselly-incompliant-earlean.ngrok-free.dev")
        ws_url = f"wss://{host}/twilio/ws"
    
    print(f"[TWILIO INCOMING] Query Params: {request.query_params}") # DEBUG
    
    # Extract Context & Prompt & Call Type
    context_encoded = request.query_params.get("context", "{}")
    prompt_encoded = request.query_params.get("prompt", "")
    call_type = request.query_params.get("call_type", "inbound")
    call_sid = request.query_params.get("CallSid") or request.query_params.get("call_sid")
    account_sid = request.query_params.get("AccountSid")
    
    # --- PHONE NUMBER EXTRACTION FIX ---
    # Inbound: 'From' is the user, 'To' is the twilio number
    # Outbound: 'To' is the user, 'From' is the twilio number
    if call_type == "outbound":
        phone_number = request.query_params.get("To") or request.query_params.get("phone") or "Unknown"
    else:
        phone_number = request.query_params.get("From") or request.query_params.get("phone") or "Unknown" 
        
    print(f"[TWILIO INCOMING] Extracted Call Type: {call_type} | Phone: {phone_number} | CallSid: {call_sid}") # DEBUG
    
    import urllib.parse
    import xml.sax.saxutils as saxutils
    
    # --- WORKFLOW ROUTING (INBOUND) ---
    if call_type == 'inbound' and account_sid:
        try:
            user_id = supabase_adapter.get_user_by_subaccount_sid(account_sid)
            if user_id:
                # Find active workflow for inbound calls
                wf = supabase_adapter.get_active_workflow_for_trigger(user_id, trigger_type='call_ended', run_on='inbound')
                if wf:
                    # PROMPT INJECTION REMOVED PER USER REQUEST
                    # Only inject workflow_id for routing
                    print(f"[TWILIO INCOMING] 🚀 Linked Inbound Workflow: {wf['name']}")
                    
                    # Inject workflow_id into context
                    try:
                        current_ctx = json.loads(urllib.parse.unquote(context_encoded) or "{}")
                    except:
                        current_ctx = {}
                        
                    current_ctx['workflow_id'] = wf['id']
                    context_encoded = urllib.parse.quote(json.dumps(current_ctx))
        except Exception as e:
            print(f"[TWILIO INCOMING] Workflow lookup error: {e}")

    try:
        # Decode Context
        context_json = urllib.parse.unquote(context_encoded)
        if not context_json: context_json = "{}"
        parsed = json.loads(context_json)
        clean_context_str = json.dumps(parsed)
        
        # Decode Prompt
        prompt_decoded = urllib.parse.unquote(prompt_encoded) if prompt_encoded else ""
        
    except Exception as e:
        print(f"[TWILIO] Invalid param: {e}")
        clean_context_str = "{}"
        prompt_decoded = ""
        
    # XML Escape (Manually escape quotes for attributes)
    context_xml_safe = saxutils.escape(clean_context_str).replace('"', '&quot;')
    prompt_xml_safe = saxutils.escape(prompt_decoded).replace('"', '&quot;')
    
    twiml_response = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Say>Connecting to AI Agent.</Say>
    <Connect>
        <Stream url="{ws_url}">
             <Parameter name="aCustomParameter" value="someValue" />
             <Parameter name="context" value="{context_xml_safe}" />
             <Parameter name="system_prompt" value="{prompt_xml_safe}" />
             <Parameter name="call_type" value="{call_type}" />
             <Parameter name="phone_number" value="{phone_number}" />
             <Parameter name="call_sid" value="{call_sid or ''}" />
             <Parameter name="call_from" value="{phone_number}" /> 
        </Stream>
    </Connect>
</Response>"""
    
    return Response(content=twiml_response, media_type="application/xml")

@app.websocket("/twilio/ws")
async def twilio_ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("[TWILIO] WS Connected", flush=True)
    
    agent = None
    inbound_buffer = bytearray()
    silence_frames = 0
    start_time = time.time()  # Track duration
    # Audio Configuration
    SAMPLE_RATE = 24000
    CHANNELS = 1
    # VAD / Barge-In Sensitivity
    # Increased to 3000 to prevent echo/noise triggering
    SILENCE_THRESHOLD = 3000 
    
    # Silence Wait: 0.8s (More conversational)
    # 40 frames * 20ms = 800ms
    SILENCE_LIMIT_FRAMES = 40 
    
    MIN_SPEECH_DURATION = 0.4 # Catch short "Yes/No"
    MIN_AUDIO_BYTES = int(16000 * 2 * MIN_SPEECH_DURATION) # 16kHz * 2 bytes/sample * seconds
    stream_sid = None
    
    # Task Tracking for Barge-In
    current_response_task = None
    
    # Call Logging
    transcript_log = []
    audio_packets_count = 0
    current_call_id = None
    user_email_for_call = None
    phone_number = "Unknown"
    current_metadata = "{}"
    last_activity_time = time.time()
    agent_finish_time = 0.0
    probe_count = 0
    
    async def play_agent_response(audio_bytes=None, text=None):
        """
        Helper to run Agent processing in background.
        Supports Cancellation (Barge-In).
        """
        nonlocal stream_sid
        nonlocal current_call_id
        # nonlocal transcript_log # Access outer list
        nonlocal last_activity_time
        nonlocal probe_count
        nonlocal agent_finish_time
        
        try:
            stream_gen = None
            if audio_bytes:
                stream_gen = agent.process_audio_stream(audio_bytes)
            elif text:
                stream_gen = agent.say(text)
                
            if stream_gen:
                async for chunk_or_text, audio_chunk in stream_gen:
                    
                    # Capture Transcript
                    if chunk_or_text and isinstance(chunk_or_text, dict):
                        if chunk_or_text.get("user"):
                            msg = f"User: {chunk_or_text['user']}"
                            print(f"[TRANSCRIPT] {msg}")
                            transcript_log.append(msg)
                            # REAL-TIME LOGGING
                            if current_call_id:
                                try:
                                    supabase_adapter.append_to_transcript(current_call_id, msg)
                                except: pass
                                 
                        if chunk_or_text.get("agent"):
                            msg = f"Agent: {chunk_or_text['agent']}"
                            # print(f"[TRANSCRIPT] {msg}")
                            transcript_log.append(msg)
                            # REAL-TIME LOGGING
                            if current_call_id:
                                try:
                                    supabase_adapter.append_to_transcript(current_call_id, msg)
                                except: pass
                        
                        if chunk_or_text.get("control") == "end_call":
                            print("[TWILIO] End Call Signal Received. Hanging up via REST API...", flush=True)
                            if call_sid_extracted:
                                try:
                                    # Force immediate disconnection on the phone
                                    provisioner = get_provisioner()
                                    t_client = provisioner.client
                                    t_client.calls(call_sid_extracted).update(status='completed')
                                    print(f"[TWILIO] REST API Hangup Success for SID: {call_sid_extracted}")
                                except Exception as tw_err:
                                    print(f"[TWILIO] REST API Hangup Error: {tw_err}")
                            
                            try:
                                await websocket.close()
                            except Exception: pass
                            return

                    if audio_chunk:
                        # Resample 24k (Agent) → 8k (Twilio)
                        audio_pcm_8k = _ratecv(audio_chunk, 24000, 8000)
                        # Convert PCM 16-bit → μ-law (what Twilio expects)
                        audio_mulaw_out = _lin2ulaw(audio_pcm_8k)
                        
                        # TRACK PLAYBACK TIME: 1 byte mulaw = 1 sample. 8000 samples = 1 second.
                        chunk_duration = len(audio_mulaw_out) / 8000.0
                        # Estimated time when this chunk will finish playing
                        agent_finish_time = max(agent_finish_time, time.time()) + chunk_duration

                        payload_out = base64.b64encode(audio_mulaw_out).decode("utf-8")
                        
                        res_msg = {
                            "event": "media",
                            "streamSid": stream_sid,
                            "media": { "payload": payload_out }
                        }
                        await websocket.send_text(json.dumps(res_msg))
                        
        except (asyncio.CancelledError, Exception) as e:
            if isinstance(e, asyncio.CancelledError):
                 print("[TWILIO] Agent Interrupted (Barge-In)", flush=True)
            else:
                 msg = str(e)
                 if "websocket.close" in msg or "already completed" in msg or "closed" in msg.lower():
                     pass # Ignore expected close errors
                 else:
                     print(f"[TWILIO] Play Error: {e}", flush=True)

            # Send Clear Message to Twilio to flush their buffer
            if stream_sid:
                try:
                    await websocket.send_text(json.dumps({
                        "event": "clear",
                        "streamSid": stream_sid
                    }))
                except: pass
            
            # On cancellation/barge-in, we stop tracking future finish time
            agent_finish_time = time.time()

    try:
        print("[Twilio] Initializing Groq Agent...", flush=True)
        
        # Load Automation Rules for Default User context if possible
        default_user = None # auth_module removed. Logic handled in Start Event.
        user_context_rules = []
        user_email_for_call = None # Ensure scope visibility
        settings = {}  # Initialize settings
        is_cal_enabled = False  # Initialize calendar flag
        
        if default_user:
             user_email_for_call = default_user  # SET EMAIL FOR AUTOMATION
             
             # Get user_id from email to load from app_options table
             try:
                 pass # import os removed
                 pass # import removed
                 pass # import os removed
                 
                 # Use admin client to lookup user by email
                 admin_client = create_client(
                     os.environ.get("SUPABASE_URL"),
                     os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
                 )
                 user_response = admin_client.auth.admin.list_users()
                 
                 user_id = None
                 for u in user_response:
                     if u.email == default_user:
                         user_id = u.id
                         break
                 
                 if user_id:
                     # Load from app_options table (correct source!)
                     settings = supabase_adapter.get_app_options(user_id) or {}
                     print(f"[Twilio] Loaded settings from app_options for user {user_id}")
                     
                     # Get automation rules using the correct key from settings
                     user_context_rules = settings.get('automation_rules', [])
                     
                     is_cal_enabled = settings.get('enable_calendar', False)
                 else:
                     print(f"[Twilio] WARN: Could not find user_id for {default_user}")
             except Exception as e:
                 print(f"[Twilio] Error loading app_options: {e}")
                 traceback.print_exc()



        # Init State
        audio_packets_count = 0
        inbound_buffer = bytearray()
        silence_frames = 0
        current_response_task = None
        current_metadata = "{}" # Initialize metadata variable
        
        # Default Agent with user settings from database AND call context
        agent = GroqAgent(
            api_key=os.environ.get("GROQ_API_KEY"), 
            output_sample_rate=24000, 
            automation_rules=user_context_rules,
            enable_calendar=is_cal_enabled,
            user_settings=settings,  # ✅ Pass settings from app_options table
        )
        print("[Twilio] Agent Ready. Waiting for events...", flush=True)

        while True:
            # 1. Receive Message
            try:
                data = await websocket.receive_text()
                msg = json.loads(data)
                # print(f"[DEBUG] WS Msg: {msg.keys()}", flush=True) # Too noisy for media?
                if msg.get('event') == 'start':
                     print(f"[DEBUG] FULL START MSG: {json.dumps(msg)}", flush=True)
            except WebSocketDisconnect:
                print("[Twilio] Client Disconnected directly.", flush=True)
                break

            event_type = msg.get("event")
            
            if event_type == "connected":
                print(f"[Twilio] Connected. Protocol: {msg.get('protocol')}", flush=True)

            elif event_type == "start":
                stream_sid = msg['start']['streamSid']
                # Try multiple case variations for CallSid
                call_sid_extracted = msg['start'].get('callSid') or msg['start'].get('CallSid')
                
                # Check customParameters fallback
                custom_params = msg['start'].get('customParameters', {})
                if not call_sid_extracted:
                    call_sid_extracted = custom_params.get('call_sid')
                
                print(f"!!! WEBSOCKET RECEIVED CALL SID: {call_sid_extracted} !!!", flush=True) # DEBUG
                call_from = msg['start'].get('customParameters', {}).get('call_from', 'Unknown')
                print(f"[Twilio] Stream Started: {stream_sid} from {call_from}", flush=True)
                print(f"[DEBUG] Raw Start Msg: {json.dumps(msg)}", flush=True)
                
                # --- USER AUTHENTICATION VIA SESSION ID ---
                custom_params = msg['start'].get('customParameters', {})
                
                # --- CALL CONTEXT EXTRACTION & AGENT UPDATE ---
                # --- CALL CONTEXT EXTRACTION & AGENT UPDATE ---
                print(f"[DEBUG] Custom Params: {custom_params}") # Added Debug
                call_context_str = custom_params.get('context', '{}')
                system_prompt = custom_params.get('system_prompt', '')
                
                # improved call_type extraction with fallback
                call_type = custom_params.get('call_type')
                if not call_type:
                    try:
                        ctx_check = json.loads(call_context_str)
                        call_type = ctx_check.get('call_type', 'inbound')
                        print(f"[DEBUG] CallType fallback to Context: {call_type}")
                    except:
                        call_type = 'inbound'
                
                print(f"[DEBUG] Final Decision Call Type: {call_type}")
                
                # --- STRICT PHONE EXTRACTION (CONSOLIDATED) ---
                if not call_sid_extracted:
                        call_sid_extracted = msg['start'].get('callSid') or msg['start'].get('CallSid')
                
                phone_number = await extract_phone_with_fallback(
                    custom_params, 
                    call_sid_extracted, 
                    msg['start'].get('accountSid'),
                    call_type
                )
                
                if phone_number == "Unknown":
                    print(f"[CRITICAL] Phone number missing for CallSid {call_sid_extracted}! Rejecting call.", flush=True)
                    # Send a rejection message if protocol allows, otherwise just close
                    await websocket.close(code=1008, reason="Phone number required")
                    return # Exit the function for this connection

                print(f"[DEBUG] Extracted Phone: {phone_number}")

                # --- METADATA PRESERVATION (START) ---
                m_payload = {
                    "call_type": call_type,
                    "call_sid": call_sid_extracted,
                    "phone_number": phone_number
                }
                current_metadata = json.dumps(m_payload)
                print(f"[DEBUG] Initial Metadata Captured: {current_metadata}")

                # Initialize call_context to empty dict to prevent NameError in finally block
                call_context = {}

                # --- REMOVED PREMATURE PROMPT LOGIC ---
                # Logic moved to specific USER RESOLUTION blocks (Active/Default)
                # to ensure we use the LATEST DB settings, not stale init settings.
                # -------------------------------------

                # --- CALL CONTEXT EXTRACTION & AGENT UPDATE ---

                session_id = custom_params.get('session_id')
                
                # Default values
                user_id = None
                user_email_for_call = 'guest@web'
                settings_full = {}
                user_token_payload = None
                
                if session_id and session_id in ACTIVE_USER_SESSIONS:
                    # Get user info from active session
                    user_session = ACTIVE_USER_SESSIONS[session_id]
                    user_id = user_session['user_id']
                    user_email_for_call = user_session['user_email']
                    # REFETCH SETTINGS FROM DB (Ensure freshness)
                    try:
                        settings_full = supabase_adapter.get_app_options(user_id) or {}
                        user_session['settings'] = settings_full # Update cache
                        print(f"[Twilio] Refetched fresh settings for {user_id}")
                    except Exception as e:
                        print(f"[Twilio] Settings refetch failed, using cache: {e}")
                        settings_full = user_session['settings']
                    user_context_rules = user_session['rules']
                    is_cal_enabled = user_session['calendar_enabled']
                    
                    print(f"[Twilio] Found session: {session_id} for User: {user_email_for_call} ({user_id})")
                    
                    # Fetch Google Tokens
                    user_token_payload = supabase_adapter.get_google_tokens(user_id)
                    
                    # --- STRICT PROMPT LOGIC FOR ACTIVE SESSION ---
                    # 1. Base Prompt Selection
                    base_instr = settings_full.get("system_instruction") or settings_full.get("system_prompt") or "You are a helpful AI assistant."
                    if call_type == 'outbound':
                        out_instr = settings_full.get("outbound_system_instruction")
                        if out_instr:
                            print(f"[Twilio] 🚀 Active Session: Switching to OUTBOUND Instruction")
                            base_instr = out_instr
                        else:
                            print(f"[Twilio] ⚠️ Active Session: Outbound call but no 'outbound_system_instruction' found. Using Generic Fallback.")
                            base_instr = "You are a helpful AI assistant. (No Outbound Prompt Configured)"
                    
                    agent.system_instruction = base_instr

                    # 2. Context & Specific Prompt
                    if call_context_str:
                         try:
                             call_ctx = json.loads(call_context_str)
                             agent.update_call_context(call_ctx)
                         except: pass

                    # 3. Add Mode Header (Optional, user wanted Clean)
                    # agent.system_instruction = f"[MODE: {call_type.upper()} CALL]\n" + agent.system_instruction

                    # 4. Specific Instruction from UI (OVERWRITES if present)
                    if system_prompt:
                         agent.update_system_prompt(system_prompt)

                    # ---------------------------------------------
                    
                    # Log Call
                    # sys_prompt optimized out
                    
                    # --- PRESERVATION FIX ---
                    # Metadata already updated above, but ensuring it's fresh for this path
                    m_payload = {
                        "call_type": call_type,
                        "call_sid": call_sid_extracted,
                        "phone_number": phone_number
                    }
                    current_metadata = json.dumps(m_payload)

                    current_call_id = perform_safe_logging(
                        user_id, 
                        call_type, 
                        call_sid_extracted, 
                        phone_number,
                        agent.system_instruction if agent else "N/A",
                        "in-progress",
                        "web-direct"
                    )
                    print(f"[LOGGING] Call Logged: {current_call_id} | Type: {call_type} (Active Session)")
                    
                else:
                    # Fallback: Use most recent active session
                    if ACTIVE_USER_SESSIONS:
                        # Get the most recent session (last item in dict)
                        recent_session_id = list(ACTIVE_USER_SESSIONS.keys())[-1]
                        recent_session = ACTIVE_USER_SESSIONS[recent_session_id]
                        
                        user_id = recent_session['user_id']
                        user_email_for_call = recent_session['user_email']
                        # REFETCH SETTINGS FROM DB (Ensure freshness)
                        try:
                            settings_full = supabase_adapter.get_app_options(user_id) or {}
                            recent_session['settings'] = settings_full # Update cache
                            print(f"[Twilio] Refetched fresh settings for {user_id} (fallback)")
                        except Exception as e:
                            print(f"[Twilio] Settings refetch failed, using cache: {e}")
                            settings_full = recent_session['settings']
                        user_context_rules = recent_session['rules']
                        is_cal_enabled = recent_session['calendar_enabled']
                        
                        print(f"[Twilio] Using most recent session: {recent_session_id} for User: {user_email_for_call} ({user_id})")
                        
                        # Fetch Google Tokens
                        user_token_payload = supabase_adapter.get_google_tokens(user_id)
                        
                        # Ensure metadata is updated for Fallback Path
                        m_payload = {
                            "call_type": call_type,
                            "call_sid": call_sid_extracted,
                            "phone_number": phone_number
                        }
                        current_metadata = json.dumps(m_payload)

                        current_call_id = perform_safe_logging(
                            user_id, 
                            call_type, 
                            call_sid_extracted, 
                            phone_number,
                            agent.system_instruction if agent else "N/A",
                            "in-progress",
                            "web-direct"
                        )
                        print(f"[LOGGING] Call Logged: {current_call_id} | Type: {call_type} (Fallback Session)")
                        
                    else:
                        # No active sessions - use default user fallback
                        print("[Twilio] No active sessions found, using default user fallback")
                        
                        # Use known user ID from your system
                        default_user_id = "655b1b48-66b6-4455-9a92-3fcac8c377eb"  # Known user ID from logs
                        
                        try:
                            print(f"[Twilio] 🟡 ATTEMPTING to fetch settings for default user: {default_user_id}")
                            settings_full = supabase_adapter.get_app_options(default_user_id) or {}
                            print(f"[Twilio] 🟢 Fetched settings keys: {list(settings_full.keys())}")
                            
                            user_token_payload = supabase_adapter.get_google_tokens(default_user_id)
                            
                            # Get user email
                            try:
                                users_response = supabase.auth.admin.list_users()
                                default_user_email = "rahulsamineni1234@gmail.com"  # Default fallback
                                
                                for user in users_response:
                                    if user.id == default_user_id:
                                        default_user_email = user.email
                                        break
                            except Exception as auth_err:
                                print(f"[Twilio] Auth Admin list_users failed: {auth_err}")
                                default_user_email = "rahulsamineni1234@gmail.com"
                            
                            user_id = default_user_id
                            user_email_for_call = default_user_email
                            
                            # Load automation rules
                            integrations = supabase_adapter.get_app_integrations(default_user_id)
                            user_context_rules = [
                                {
                                    "id": i.get("id"),
                                    "service": i.get("service_type"),
                                    "is_active": i.get("is_active"),
                                    **(i.get("config_json") or {})
                                } for i in integrations
                            ]
                            
                            # Calendar Toggle
                            raw_cal = settings_full.get("enable_calendar", False)
                            is_cal_enabled = (raw_cal is True or str(raw_cal).lower() == "true")
                            
                            print(f"[Twilio] Using default user: {default_user_email} ({user_id})")
                            
                            # Log Call with correct Metadata
                            # STRICT PROMPT SELECTION FOR DEFAULT USER
                            base_instr = settings_full.get("system_instruction") or settings_full.get("system_prompt") or "You are a helpful AI assistant."
                            
                            if call_type == 'outbound':
                                out_instr = settings_full.get("outbound_system_instruction")
                                if out_instr:
                                    print(f"[Twilio] 🚀 Default User: Switching to OUTBOUND Instruction")
                                    base_instr = out_instr
                                else:
                                    print(f"[Twilio] ⚠️ Default User: Outbound call but no 'outbound_system_instruction' found. Using Generic Fallback.")
                                    base_instr = "You are a helpful AI assistant. (No Outbound Prompt Configured)"
                            
                            # Update Agent Instance (CRITICAL FIX)
                            agent.system_instruction = base_instr
                            
                            if system_prompt:
                                 agent.update_system_prompt(system_prompt)


                            # Update Context if available
                            if call_context_str:
                                 try:
                                     call_ctx = json.loads(call_context_str)
                                     agent.update_call_context(call_ctx)
                                 except: pass

                            # Ensure metadata is updated for Default User Path
                            m_payload = {
                                "call_type": call_type,
                                "call_sid": call_sid_extracted,
                                "phone_number": phone_number
                            }
                            current_metadata = json.dumps(m_payload)

                            current_call_id = perform_safe_logging(
                                user_id, 
                                call_type, 
                                call_sid_extracted, 
                                phone_number,
                                agent.system_instruction if agent else "N/A",
                                "in-progress",
                                "twilio-default"
                            )
                            print(f"[LOGGING] Call Logged: {current_call_id} | Type: {call_type} (Default User)")
                            
                        except Exception as e:
                            print(f"[Twilio] Default user setup failed: {e}")
                            # Ultimate fallback
                            user_id = None
                            user_email_for_call = "guest@web"
                            settings_full = {}
                            user_context_rules = []
                            is_cal_enabled = False
                
                print(f"[Twilio] Starting Session for User ID: {user_id} ({user_email_for_call})")

                # 5. Initialize Agent
                agent = GroqAgent(
                    api_key=GROQ_KEY,
                    phone=phone_number if phone_number != "Unknown" else None,
                    enable_calendar=is_cal_enabled,
                    user_settings=settings_full,
                    automation_rules=user_context_rules,
                    user_token=user_token_payload
                )

                # Ensure second agent also uses Outbound Base Prompt if needed
                if call_type == 'outbound' and agent:
                    out_base = settings_full.get("outbound_system_prompt")
                    if out_base:
                        print(f"[Twilio] 🚀 Switching to OUTBOUND Base Prompt (Re-init)")
                        agent.system_instruction = out_base

                # Re-apply Call Context & System Prompt to the NEW Agent
                if call_context:
                    agent.update_call_context(call_context)
                
                if system_prompt:
                    agent.update_system_prompt(system_prompt)

                
                # current_response_task = asyncio.create_task(play_agent_response(text="Namaste! Welcome to Premium Cars. Which language do you prefer?"))
                
                # Dynamic Greeting Task
                async def dynamic_intro():
                     greeting_text = await agent.generate_greeting()
                     if greeting_text:
                         await play_agent_response(text=greeting_text)
                
                current_response_task = asyncio.create_task(dynamic_intro())
                
            elif event_type == "media":
                audio_packets_count += 1
                payload_base64 = msg["media"]["payload"]
                audio_mulaw = base64.b64decode(payload_base64)
                
                # Decode μ-law → PCM, then resample 8k → 16k for Groq STT
                audio_pcm_8k = _ulaw2lin(audio_mulaw)
                audio_pcm_16k = _ratecv(audio_pcm_8k, 8000, 16000)
                
                # Check RMS for Barge-In (user talking while AI is speaking)
                rms = _rms(audio_pcm_16k)
                if rms > SILENCE_THRESHOLD:
                    # ** BARGE-IN TRIGGER **
                    if current_response_task and not current_response_task.done():
                        print("[TWILIO] User Speaking - Cancelling Agent...", flush=True)
                        current_response_task.cancel()
                    
                    silence_frames = 0
                    inbound_buffer.extend(audio_pcm_16k)
                    
                    # USER ACTIVITY DETECTED
                    last_activity_time = time.time()
                    probe_count = 0
                else:
                    silence_frames += 1
                    if len(inbound_buffer) > 0: 
                        inbound_buffer.extend(audio_pcm_16k)
                
                # Silence logic (Processing User Speech)
                if silence_frames > SILENCE_LIMIT_FRAMES:
                    if len(inbound_buffer) > MIN_AUDIO_BYTES:
                        print(f"[Twilio] User Spoke: {len(inbound_buffer)} bytes.", flush=True)
                        
                        # Wrap in WAV
                        wav_io = io.BytesIO()
                        with wave.open(wav_io, 'wb') as wav_f:
                            wav_f.setnchannels(1)
                            wav_f.setsampwidth(2)
                            wav_f.setframerate(16000)
                            wav_f.writeframes(inbound_buffer)
                        wav_io.seek(0)
                        audio_bytes = wav_io.read()
                        
                        current_response_task = asyncio.create_task(play_agent_response(audio_bytes=audio_bytes))
                        last_activity_time = time.time() # Reset on dispatch
                        probe_count = 0

                        inbound_buffer = bytearray()
                        silence_frames = 0
                    elif len(inbound_buffer) > 0:
                        inbound_buffer = bytearray()
                        silence_frames = 0
                
                # --- AUTO-HANGUP / SILENCE PROBE LOGIC ---
                is_agent_speaking = (current_response_task and not current_response_task.done())
                is_audio_playing = (time.time() < agent_finish_time)

                if is_agent_speaking or is_audio_playing:
                    # Agent is active or audio buffer is still playing
                    last_activity_time = time.time()
                else:
                    # Agent is truly idle - check for user silence
                    silence_duration = time.time() - last_activity_time
                    
                    if silence_duration > 7.0:
                        if probe_count >= 2:
                            print(f"[TWILIO] Silence duration {silence_duration:.1f}s after {probe_count} probes. Hanging up.", flush=True)
                            if call_sid_extracted:
                                try:
                                    provisioner = get_provisioner()
                                    t_client = provisioner.client
                                    t_client.calls(call_sid_extracted).update(status='completed')
                                except Exception as e: print(f"Auto-Hangup Error: {e}")
                            break
                        else:
                            print(f"[TWILIO] Silence detected for {silence_duration:.1f}s. Probing user (Attempt {probe_count + 1})...", flush=True)
                            probe_count += 1
                            last_activity_time = time.time() 
                            current_response_task = asyncio.create_task(play_agent_response(text="Hello, are you still there?"))

            elif event_type == "stop":
                print(f"[TWILIO] Stream Stopped: {stream_sid}", flush=True)
                break

    except WebSocketDisconnect:
        print("[TWILIO] WS Disconnected", flush=True)
    except Exception as e:
        print(f"[TWILIO] Error: {e}", flush=True)
        # import traceback
        # traceback.print_exc()
    finally:
        print("[DEBUG] Finally block started!", flush=True)
        print(f"[DEBUG] current_call_id = {current_call_id}", flush=True)
        print(f"[DEBUG] user_email_for_call = {user_email_for_call}", flush=True)
        
        full_transcript = "" # Pre-initialize to avoid UnboundLocalError
        
        # END CALL LOGGING (TWILIO)
        # END CALL LOGGING (TWILIO)
        if current_call_id:
            try:
                # Capture Transcript & Duration
                duration_sec = int(time.time() - start_time)
                
                # PRESERVE TRANSCRIPT: Calculate from history
                full_transcript = "\n".join([f"{x['role']}: {x['content']}" for x in agent.history]) if agent else ""
                
                # Ensure we don't save an empty transcript which prevents automation
                if not full_transcript or full_transcript.strip() == "":
                    full_transcript = "[Call Ended - No speech detected or transcript empty]"
                
                print(f"[LOGGING] Ending Call ID: {current_call_id} | Dur: {duration_sec}s | TransLen: {len(full_transcript)}", flush=True)
                
                # Save Final State
                final_user_id = user_id
                if not final_user_id and 'automation_user_id' in locals():
                    final_user_id = automation_user_id

                supabase_adapter.log_call(
                    user_id=final_user_id or user_id, 
                    transcript=full_transcript,
                    status="completed",
                    system_prompt=agent.system_instruction if agent else "N/A",
                    connected_account="twilio-direct",
                    metadata=current_metadata,
                    call_id=current_call_id
                )
            except Exception as e:
                print(f"[ERROR] Failed to save transcript: {e}", flush=True)
            
            # Legacy Status Update (Redundant but safe to keep or remove - log_call handles it)
            # supabase_adapter.update_call_status(current_call_id, "completed")
            
            # --- POST CALL AUTOMATION (UNIFIED) ---
            # Try to get user_id from the call record first
            try:
                # Trigger automation based on whichever user_id was actually used
                automation_user_id = user_id
                if not automation_user_id:
                     # Check if we can recover from active session or call record
                     call_rec = supabase_adapter.get_call_by_id(current_call_id)
                     if call_rec:
                         automation_user_id = call_rec.get('user_id')

                if automation_user_id:
                    print(f"[AUTO] Triggering Post-Call Processing for User ID: {automation_user_id}", flush=True)
                    task = asyncio.create_task(automation_engine.process_call_background(current_call_id, automation_user_id))
                    
                    def task_done_callback(t):
                        try:
                            t.result()
                            print(f"[AUTO] Background analysis task for {current_call_id} completed successfully", flush=True)
                        except asyncio.CancelledError:
                            print(f"[AUTO] Background analysis task for {current_call_id} was cancelled", flush=True)
                        except Exception as e:
                            print(f"[AUTO ERROR] Background analysis failed for {current_call_id}: {e}", flush=True)
                    
                    task.add_done_callback(task_done_callback)
                else:
                    print(f"[WARN] No valid user_id found for automation. user_email_for_call={user_email_for_call}", flush=True)
                    
            except Exception as e:
                print(f"[ERROR] Failed to trigger automation: {e}", flush=True)

        elif audio_packets_count > 50:
            # Fallback for short calls that failed to start properly but had audio
            print(f"[TWILIO] Audio detected but no clean exit. Creating fallback call log...", flush=True)
            
            # Try to get user info for fallback logging
            fallback_user_id = None
            if user_email_for_call and user_email_for_call != "guest@web":
                try:
                    users_response = supabase.auth.admin.list_users()
                    for user in users_response:
                        if user.email == user_email_for_call:
                            fallback_user_id = user.id
                            break
                except:
                    pass
            
            if fallback_user_id:
                # Create a fallback call log
                fallback_call_id = supabase_adapter.log_call(
                    user_id=fallback_user_id,
                    transcript="[Call ended abruptly - fallback logging]",
                    status="completed",
                    system_prompt="Fallback processing"
                )
                
                if fallback_call_id:
                    print(f"[TWILIO] Fallback call created with ID: {fallback_call_id}", flush=True)
                    print(f"[AUTO] Triggering Post-Call Processing for fallback call...", flush=True)
                    
                    task = asyncio.create_task(automation_engine.process_call_background(fallback_call_id, fallback_user_id))
                    
                    def task_done_callback(t):
                        try:
                            t.result()
                            print(f"[AUTO] Fallback background task completed successfully", flush=True)
                        except asyncio.CancelledError:
                            print(f"[AUTO] Fallback background task was cancelled", flush=True)
                        except Exception as e:
                            print(f"[AUTO ERROR] Fallback background task failed: {e}", flush=True)
                    
                    task.add_done_callback(task_done_callback)
                else:
                    print(f"[TWILIO] Failed to create fallback call log", flush=True)
            else:
                print("[TWILIO] No valid user found for fallback automation", flush=True)

                print("[TWILIO] No valid user found for fallback automation", flush=True)
#        if current_call_id:
#            try:
#                final_status = "completed"
#                
#                # Retrieve call_context safely (it might be empty if initialization failed)
#                # Defined above to avoid NameError
#                
#                # Calculate Duration
#                duration_sec = int(time.time() - start_time)
#                
#                full_transcript = "\n".join([f"{x['role']}: {x['content']}" for x in agent.history])
#                print(f"[LOGGING] Ending Call ID: {current_call_id} | Duration: {duration_sec}s | TransLen: {len(full_transcript)}")
#                
#                supabase_adapter.log_call(
#                    user_id=user_id, # Might be None?
#                    transcript=full_transcript,
#                    status=final_status,
#                    system_prompt=agent.system_instruction if agent else "N/A",
#                    connected_account="twilio-direct"
#                )
#            except Exception as e:
#                print(f"[ERROR] Finally block error: {e}")

# --- BROWSER SIMULATOR ENDPOINT ---
@app.websocket("/browser-simulator")
async def browser_ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("[BROWSER] WS Connected", flush=True)
    
    # Call State
    current_call_id = None
    start_time = time.time()
    current_metadata = "{}"
    user_id = None
    agent = None

    try:
        # 1. Wait for Config
        config_data = await websocket.receive_text()
        config = json.loads(config_data)
        print(f"[BROWSER] Config Received: {config}", flush=True)

        user_token = config.get("token")
        
        # Verify Token / User
        if user_token == "oauth_success_token":
             user_id = config.get("user_id")
             user_email = config.get("user_email")
             print(f"[BROWSER] OAuth User: {user_id}", flush=True)
        else:
             try:
                user_response = supabase.auth.get_user(user_token)
                if user_response and user_response.user:
                    user_id = user_response.user.id
                    print(f"[BROWSER] Supabase User: {user_id}", flush=True)
             except:
                 print("[BROWSER] Invalid User Token", flush=True)
        
        # Load Settings
        settings = {}
        if user_id:
            settings = supabase_adapter.get_app_options(user_id) or {}
            print(f"[BROWSER] Loaded Settings for {user_id}")
        
        # Override with simulator phone
        sim_phone = config.get("phone", "Unknown")
        if sim_phone and sim_phone != "Unknown":
            settings['phone_number'] = sim_phone

        # Initialize Agent
        enable_calendar = settings.get('enable_calendar', False)
        
        agent = GroqAgent(
            api_key=GROQ_KEY,
            phone=settings.get('phone_number', 'Unknown'),
            automation_rules=[], 
            enable_calendar=enable_calendar,
            user_settings=settings,
            user_token=user_token
        )

        # --- BROWSER CALL LOGGING ---
        if user_id:
            phone_num = settings.get('phone_number', 'Unknown')
            current_metadata = json.dumps({
                "call_type": "inbound",
                "call_sid": f"browser-{int(time.time())}",
                "phone_number": phone_num
            })
            current_call_id = perform_safe_logging(
                user_id,
                "inbound",
                f"browser-{int(time.time())}",
                phone_num,
                agent.system_instruction,
                "in-progress",
                "browser-simulator"
            )
            print(f"[BROWSER] Call Logged: {current_call_id} | Phone: {phone_num}")
        
        # Send Ready Signal
        await websocket.send_json({"status": "ready", "message": "Agent Initialized"})

        # Initial Greeting (Capture in Transcript)
        greeting = await agent.generate_greeting()
        if greeting:
             await websocket.send_json({"type": "agent_text", "payload": greeting})
             # Audio for greeting
             async for data, audio in agent.say(greeting):
                 if audio: await websocket.send_bytes(audio)

        # Audio Buffer
        audio_buffer = bytearray()

        # 2. Main Loop
        while True:
            message = await websocket.receive()
            if "text" in message:
                data = json.loads(message["text"])
                if data.get("type") == "text_input":
                    user_text = data.get("text")
                    print(f"[BROWSER] User Text: {user_text}", flush=True)
                    await websocket.send_json({"status": "processing"})
                    async for data, audio in agent.process_text_stream(user_text):
                        if data:
                            if isinstance(data, dict) and "agent" in data:
                                await websocket.send_json({"type": "agent_text", "payload": data["agent"]})
                            elif isinstance(data, str):
                                await websocket.send_json({"type": "agent_text", "payload": data})
                        if audio: await websocket.send_bytes(audio)
                    await websocket.send_json({"status": "ready"})
                elif data.get("type") == "audio_end":
                    if len(audio_buffer) > 0:
                        await websocket.send_json({"status": "processing"})
                        try:
                            async for data, audio in agent.process_audio_stream(bytes(audio_buffer)):
                                if data:
                                    if isinstance(data, dict) and "agent" in data:
                                        await websocket.send_json({"type": "agent_text", "payload": data["agent"]})
                                    elif isinstance(data, str):
                                        await websocket.send_json({"type": "agent_text", "payload": data})
                                if audio: await websocket.send_bytes(audio)
                        except Exception as e:
                            print(f"[BROWSER] Audio Processing Error: {e}")
                        audio_buffer = bytearray()
                        await websocket.send_json({"status": "ready"})
            elif "bytes" in message:
                audio_buffer.extend(message["bytes"])

    except WebSocketDisconnect:
        print("[BROWSER] WS Disconnected", flush=True)
    except Exception as e:
        print(f"[BROWSER] Error: {e}", flush=True)
        traceback.print_exc()
    finally:
        # BROWSER END CALL LOGGING
        if current_call_id and user_id:
            try:
                full_transcript = "\n".join([f"{x['role']}: {x['content']}" for x in agent.history]) if agent else ""
                if not full_transcript or full_transcript.strip() == "":
                    full_transcript = "[Call Ended - No speech detected or transcript empty]"
                
                print(f"[BROWSER] Finalizing Call {current_call_id} | TransLen: {len(full_transcript)}")
                
                supabase_adapter.log_call(
                    user_id=user_id,
                    transcript=full_transcript,
                    status="completed",
                    system_prompt=agent.system_instruction if agent else "N/A",
                    connected_account="browser-simulator",
                    metadata=current_metadata,
                    call_id=current_call_id
                )
                
                # Trigger Automation
                print(f"[BROWSER] Triggering Post-Call Analysis for {current_call_id}")
                asyncio.create_task(automation_engine.process_call_background(current_call_id, user_id))
            except Exception as le:
                print(f"[BROWSER ERROR] Logging Finally: {le}")

templates = Jinja2Templates(directory="templates")

@app.get("/login")
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.get("/dashboard")
async def dashboard(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/workflows")
async def workflows(request: Request):
    return templates.TemplateResponse("workflows.html", {"request": request})

@app.get("/")
async def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# --- API Endpoints ---


@app.post("/api/config")
async def save_config(config: dict, user: dict = Depends(get_current_user)):
    email = user.email
    from backend.supabase_client import supabase_adapter
    
    # Merge existing integrations? No, Integrations are separate table.
    # This endpoint handles General Options.
    
    success = supabase_adapter.save_app_options(user.id, config)
    
    if success:
        return {"status": "success"}
    
    return JSONResponse(status_code=500, content={"status": "error"})

@app.get("/api/config")
async def get_config(user: dict = Depends(get_current_user)):
    from backend.supabase_client import supabase_adapter
    
    # 1. Get General Options
    options = supabase_adapter.get_app_options(user.id)
    
    # 2. Skip legacy Automation Rules (Integrations)
    # integrations = supabase_adapter.get_app_integrations(user.id)
    
    # 3. Format Integrations for Frontend (REMOVED)
    # Combine
    # options["automation_rules"] = rules_list
    
    return options


@app.post("/api/save_calendar")
async def save_calendar(request: Request, user: dict = Depends(get_current_user)):
    """
    Saves just the calendar toggle setting without affecting other options.
    """
    from backend.supabase_client import supabase_adapter
    
    body = await request.json()
    enable_calendar = body.get("enable_calendar", False)
    
    # Get existing options first to preserve them
    options = supabase_adapter.get_app_options(user.id)
    
    # Update only the calendar setting, keep everything else
    if options is None:
        options = {}
    
    options["enable_calendar"] = enable_calendar
    
    # Save back using config_dict format
    supabase_adapter.save_app_options(user.id, options)
    
    print(f"[Calendar] Toggle set to {enable_calendar} for user {user.id}")
    return {"success": True, "enable_calendar": enable_calendar}

@app.delete("/api/integrations/{rule_id}")
async def delete_integration(rule_id: str, user: dict = Depends(get_current_user)):
    from backend.supabase_client import supabase_adapter
    
    success = supabase_adapter.delete_app_integration(user.id, rule_id)
    
    if success:
         return {"status": "success"}
    return JSONResponse(status_code=500, content={"status": "error", "message": "Invalid index or save failed"})

@app.get("/api/user/profile")
async def get_user_profile(user: dict = Depends(get_current_user)):
    """Returns the authenticated user's profile data."""
    try:
        print(f"[PROFILE DEBUG] User Object Type: {type(user)}")
        print(f"[PROFILE DEBUG] User Dict: {user}")
        
        email = getattr(user, 'email', "No Email")
        metadata = getattr(user, 'user_metadata', {})
        
        print(f"[PROFILE DEBUG] Email: {email}")
        print(f"[PROFILE DEBUG] Metadata: {metadata}")
        
        return {
            "status": "success",
            "email": email,
            "metadata": metadata
        }
    except Exception as e:
        print(f"[PROFILE DEBUG] ERROR: {e}")
        return {
            "status": "error",
            "message": str(e),
            "email": "error@example.com",
            "metadata": {}
        }

@app.post("/api/auth/sync_google")
async def sync_google_auth(payload: dict, user: dict = Depends(get_current_user)):
    """Stores the Google OAuth tokens (Access + Refresh) in the database"""
    print(f"[AUTH DEBUG] Sync Request Payload: {payload.keys()}") # DEBUG
    provider_token = payload.get("provider_token")
    provider_refresh_token = payload.get("provider_refresh_token")
    
    if not provider_token:
         print("[AUTH DEBUG] Missing provider_token!")
         return JSONResponse(status_code=400, content={"status": "error", "message": "Missing provider_token"})
    
    # Save to Database via Supabase Adapter
    token_data = {
        "access_token": provider_token,
        "refresh_token": provider_refresh_token,
        "expires_in": payload.get("expires_in"), # Optional
        "token_type": payload.get("token_type", "Bearer")
    }
    print(f"[AUTH DEBUG] Token Data Prepared: {token_data.keys()}")
    
    from backend.supabase_client import supabase_adapter
    success = supabase_adapter.save_user_token(user.id, token_data)
    
    if success:
        print(f"[AUTH] Google Tokens Synced for {user.email}")
        return {"status": "success"}
    else:
        return JSONResponse(status_code=500, content={"status": "error", "message": "Failed to save tokens"})

@app.post("/api/generate_prompt")
async def generate_prompt(payload: dict, user: dict = Depends(get_current_user)):
    user_goal = payload.get("goal", "")
    current_prompt = payload.get("current_prompt", "")
    
    if not user_goal:
        return JSONResponse(status_code=400, content={"status": "error", "message": "Goal is required"})
        
    try:
        # Use Groq Client
        from groq import Groq
        client = Groq(api_key=os.environ.get("GROQ_API_KEY"))
        
        system_generator_prompt = "You are an expert Prompt Engineer for AI Agents. Output ONLY the System Prompt text for the user's goal."
        
        user_message = f"User Goal: {user_goal}\nCurrent Prompt: {current_prompt}"
        
        completion = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[
                {"role": "system", "content": system_generator_prompt},
                {"role": "user", "content": user_message}
            ],
            temperature=0.7,
            max_tokens=1024
        )
        
        generated_prompt = completion.choices[0].message.content
        return {"status": "success", "prompt": generated_prompt}
        
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})

# --- GOOGLE SHEETS API ---

@app.get("/api/sheets")
async def get_sheets(user: dict = Depends(get_current_user)):
    """Fetch list of Google Sheets"""
    try:
        from automation_engine import get_google_access_token, list_google_sheets
        from backend.supabase_client import supabase_adapter
        
        # Get tokens
        tokens = supabase_adapter.get_google_tokens(user.id)
        if not tokens:
            return JSONResponse(status_code=400, content={"status": "error", "message": "Google account not connected"})
            
        access_token = await get_google_access_token(tokens)
        if not access_token:
            return JSONResponse(status_code=401, content={"status": "error", "message": "Failed to refresh Google token"})
            
        files = await list_google_sheets(access_token)
        return {"status": "success", "files": files}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})

@app.get("/api/sheets/{sheet_id}/columns")
async def get_sheet_columns(sheet_id: str, user: dict = Depends(get_current_user)):
    """Fetch columns from the first sheet of a Google Spreadsheet"""
    try:
        from automation_engine import get_google_access_token, read_google_sheets_structure
        from backend.supabase_client import supabase_adapter
        
        # Get tokens
        tokens = supabase_adapter.get_google_tokens(user.id)
        if not tokens:
            return JSONResponse(status_code=400, content={"status": "error", "message": "Google account not connected"})
            
        access_token = await get_google_access_token(tokens)
        if not access_token:
            return JSONResponse(status_code=401, content={"status": "error", "message": "Failed to refresh Google token"})
            
        # Read structure (reuse existing engine function)
        structure = await read_google_sheets_structure(sheet_id, access_token)
        
        if "error" in structure:
             return JSONResponse(status_code=500, content={"status": "error", "message": structure["error"]})
             
        # Extract columns from first sheet
        sheets = structure.get("sheets", [])
        if not sheets:
            return JSONResponse(status_code=404, content={"status": "error", "message": "No sheets found"})
            
        first_sheet = sheets[0]
        columns = first_sheet.get("columns", [])
        
        return {"status": "success", "columns": columns, "sheet_name": first_sheet.get("title")}
        
    except Exception as e:
        print(f"[API ERROR] Failed to fetch columns: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    
    # 1. Handshake (Get API Key)
    api_key = None
    phone = None
    user_email = None # Capture Email for later
    transcript_log = [] # Capture Conversation
    call_id = None # Initialize call_id for logging
    try:
        msg = await websocket.receive_text()
        data = json.loads(msg)
        print(f"[WS] DEBUG CONFIG: {data}") # DEBUG token receipt
        if data.get("type") == "config":
            api_key = data.get("key")
            phone = data.get("phone")
            token = data.get("token")
            user_id_explicit = data.get("user_id") # Explicit ID
            
            # --- AUTH & CONTEXT LOADING ---
            user_context_rules = []
            is_cal_enabled = False # Default False
            settings = {} # Init default
            
            # 1. Resolve User ID (Supabase UUID is primary key)
            user_id = user_id_explicit  # This should be the Supabase UUID
            print(f"[WS] Supabase user_id received: {user_id}")
            
            if not user_id and token:
                print(f"[WS] No explicit user_id, trying token: {token[:20] if len(token) > 20 else token}...")
                try:
                    # Handle OAuth success token - user_id should be in the config message
                    if token == "oauth_success_token":
                        # Use the Supabase UUID sent from frontend (from localStorage)
                        user_id = user_id_explicit  # This is the Supabase UUID from OAuth
                        user_email = data.get("user_email")  # Email is for display/logging only
                        print(f"[WS] Using OAuth Supabase user_id: {user_id} (email: {user_email})")
                    else:
                        # Try JWT decode for real Supabase tokens
                        payload = supabase_adapter.decode_jwt(token)
                        user_id = payload.get("sub")  # This is the Supabase UUID
                        email = payload.get("email")
                        if email: user_email = email
                        if user_id: print(f"[WS] Supabase user_id from JWT: {user_id}")
                except Exception as e:
                    print(f"[WS] Token Verification Failed: {e}")
            
            print(f"[WS] Final resolved Supabase user_id: {user_id}, user_email: {user_email}")

            # 2. Load User Context
            if user_id:
                try:
                    # Load from app_options table
                    settings = supabase_adapter.get_app_options(user_id) or {}
                    print(f"[WS] Loaded settings from app_options for user {user_id}")
                    
                    # Legacy automation rules loading REMOVED.
                    user_context_rules = []
                    
                    # Calendar Toggle (KEEP for real-time tools)
                    raw_cal = settings.get("enable_calendar", False)
                    is_cal_enabled = (raw_cal is True or str(raw_cal).lower() == "true")
                    
                    print(f"[WS] Legacy Rules skipped. Calendar Enabled: {is_cal_enabled}")

                    
                    # 3. Load Google Tokens (Critical for Agent Calendar Tools)
                    if is_cal_enabled:
                        try:
                            google_tokens = supabase_adapter.get_google_tokens(user_id)
                            if google_tokens:
                                settings['google_tokens'] = google_tokens
                                print(f"[WS] Injected Google Tokens for User {user_id}")
                            else:
                                print(f"[WS] WARNING: Calendar enabled but NO Google Tokens found for {user_id}")
                        except Exception as e:
                             print(f"[WS] Error fetching Google Tokens: {e}")
                    
                    # Store user session for Twilio integration
                    import uuid
                    session_id = str(uuid.uuid4())
                    ACTIVE_USER_SESSIONS[session_id] = {
                        "user_id": user_id,
                        "user_email": user_email,
                        "settings": settings,
                        "rules": user_context_rules,
                        "calendar_enabled": is_cal_enabled
                    }
                    print(f"[WS] Stored user session: {session_id} for {user_email}")
                    
                    # Send session ID to frontend for Twilio calls
                    await websocket.send_json({
                        "status": "connected",
                        "session_id": session_id
                    })
                    
                except Exception as e:
                    print(f"[WS] Context Load Error: {e}")
            else:
                 print("[WS] No User ID resolved. Using Guest Defaults.")
                 await websocket.send_json({"status": "connected"})
        else:
            await websocket.close(code=1003, reason="Expected Config")
            return
            
        # 2. Init Agent (Groq + Cartesia)
        print(f"[INFO] Initializing Groq Agent (Cartesia)... Phone: {phone}")
        agent = GroqAgent(api_key=api_key, phone=phone, automation_rules=user_context_rules, enable_calendar=is_cal_enabled, user_settings=settings)
        
        await websocket.send_json({"status": "ready"})

        if user_id:  # Use the Supabase UUID directly, no email lookup needed
            # RE-VERIFY TOKENS for Debugging
            if is_cal_enabled:
                 print(f"[WS-DEBUG] Calendar Enabled for User: {user_id}")
                 if not settings.get('google_tokens'):
                     print(f"[WS-DEBUG] Settings missing tokens, attempting refetch...")
                     try:
                         refetched_tokens = supabase_adapter.get_google_tokens(user_id)
                         if refetched_tokens:
                             settings['google_tokens'] = refetched_tokens
                             print(f"[WS-DEBUG] Refetched Tokens successfully: {list(refetched_tokens.keys())}")
                         else:
                             print(f"[WS-DEBUG] Refetch returned None/Empty")
                     except Exception as e:
                         print(f"[WS-DEBUG] Refetch failed: {e}")
                 else:
                     print(f"[WS-DEBUG] Settings already has tokens: {list(settings.get('google_tokens').keys())}")

        # Start call logging for WS
        if user_id:  # Use the Supabase UUID directly, no email lookup needed
            current_sys_prompt = agent.system_instruction if agent else "N/A"
            auto_meta = json.dumps(user_context_rules) if user_context_rules else "[]"
            call_id = supabase_adapter.log_call(
                user_id=user_id,  # This is the Supabase UUID
                transcript="[WS Call Started]",
                status="in-progress",
                connected_account="websocket",
                system_prompt=current_sys_prompt,
                metadata=auto_meta
            )
            if call_id:
                print(f"[WS LOGGING] Call ID: {call_id}")
        
        # 3. Audio/Text Loop
        while True:
            try:
                message = await websocket.receive()
            except Exception as e:
                print(f"[WS] Receive Error: {e}")
                break

            # --- TEXT FRAME (Native STT) ---
            if "text" in message and message["text"]:
                data = message["text"]
                try:
                    msg = json.loads(data)
                    if msg.get("type") == "config":
                        print(f"[INFO] Config: {msg}")
                        if msg.get("phone"): agent.phone = msg.get("phone")
                    
                    elif msg.get("type") == "text_input":
                        user_text = msg.get("text")
                        print(f"[SERVER] Received Text: {user_text}")
                        # Process Text Stream
                        await websocket.send_json({"status": "processing"})
                        async for text_data, audio_chunk in agent.process_text_stream(user_text):
                            if text_data and isinstance(text_data, dict):
                                if text_data.get("user"):
                                    transcript_log.append(f"User: {text_data['user']}") # Log
                                    if call_id: supabase_adapter.append_to_transcript(call_id, f"User: {text_data['user']}")
                                    await websocket.send_json({"type": "user_text", "payload": text_data["user"]})
                                if text_data.get("agent"):
                                    transcript_log.append(f"Agent: {text_data['agent']}") # Log
                                    if call_id: supabase_adapter.append_to_transcript(call_id, f"Agent: {text_data['agent']}")
                                    await websocket.send_json({"type": "agent_text", "payload": text_data["agent"]})
                                
                                if text_data.get("control") == "end_call":
                                    print("[WS] End Call Signal Received. Closing connection.")
                                    await websocket.close()
                                    return
                            if audio_chunk:
                                duration = len(audio_chunk) / 48000.0
                                agent_finish_time = max(agent_finish_time, time.time()) + duration
                                await websocket.send_bytes(audio_chunk)
                        await websocket.send_json({"status": "ready"})
                        # Reset timer after agent finishes responding
                        last_activity_time = time.time()
                        probe_sent = False
                        
                except json.JSONDecodeError:
                    print(f"[WARN] Invalid JSON: {data}")

            # --- BINARY FRAME (Audio Blob) ---
            elif "bytes" in message and message["bytes"]:
                audio_bytes = message["bytes"]
                # Process Audio Stream
                await websocket.send_json({"status": "processing"})
                async for text_data, audio_chunk in agent.process_audio_stream(audio_bytes):
                     if text_data and isinstance(text_data, dict):
                        if text_data.get("user"):
                            transcript_log.append(f"User: {text_data['user']}") # Log
                            if call_id: supabase_adapter.append_to_transcript(call_id, f"User: {text_data['user']}")
                            await websocket.send_json({"type": "user_text", "payload": text_data["user"]})
                        if text_data.get("agent"):
                            transcript_log.append(f"Agent: {text_data['agent']}") # Log
                            if call_id: supabase_adapter.append_to_transcript(call_id, f"Agent: {text_data['agent']}")
                            await websocket.send_json({"type": "agent_text", "payload": text_data["agent"]})
                        
                        if text_data.get("control") == "end_call":
                            print("[WS] End Call Signal Received (Audio Path). Closing connection.")
                            await websocket.close()
                            return
                     if audio_chunk:
                        duration = len(audio_chunk) / 48000.0
                        agent_finish_time = max(agent_finish_time, time.time()) + duration
                        await websocket.send_bytes(audio_chunk)
                await websocket.send_json({"status": "ready"})
                # Reset timer after agent finishes responding
                last_activity_time = time.time()
                probe_count = 0
                
    except WebSocketDisconnect:
        print("WS Disconnected")
    except Exception as e:
        print(f"WS Error: {e}")
        try:
            await websocket.close()
        except:
            pass
    finally:
        # END CALL LOGIC
        if user_email and len(transcript_log) > 0:
            full_transcript = "\n".join(transcript_log)
            if not full_transcript or full_transcript.strip() == "":
                full_transcript = "[Call Ended - No transcript captured]"
            
            print(f"[WS] Call Ended. Logging {len(transcript_log)} lines for {user_email}")
            if call_id:
                supabase_adapter.update_call_status(call_id, "completed")
                task = asyncio.create_task(automation_engine.process_call_background(call_id, user_email))
                
                def task_done_callback(t):
                    try:
                        t.result()
                        print(f"[AUTO] Browser background analysis for {call_id} successful", flush=True)
                    except asyncio.CancelledError:
                        print(f"[AUTO] Browser background analysis for {call_id} was cancelled", flush=True)
                    except Exception as e:
                        print(f"[AUTO ERROR] Browser background analysis failed for {call_id}: {e}", flush=True)
                
                task.add_done_callback(task_done_callback)
        elif call_id:
            # If call_id exists but no transcript, mark as failed or incomplete
            supabase_adapter.update_call_status(call_id, "failed")
            print(f"[WS] Call ID {call_id} ended with no transcript. Marked as failed.")


@app.get("/api/oauth/google")
async def google_oauth_login(request: Request):
    """Public OAuth endpoint for login - no authentication required"""
    try:
        client_id = os.environ.get("GOOGLE_CLIENT_ID")
        # Construct redirect_uri from request URL to ensure exact match with callback
        base_url = str(request.url).split('/api/oauth/google')[0]  # Get base URL
        redirect_uri = f"{base_url}/api/oauth/google/callback"
        print(f"[OAuth Login] Using Redirect URI: {redirect_uri}")
        print(f"[OAuth Login] Request URL: {request.url}")
        
        if not client_id:
            raise HTTPException(status_code=500, detail="Missing GOOGLE_CLIENT_ID in env")

        # Login flow needs openid/email/profile scopes in addition to API scopes
        scope = "openid email profile https://www.googleapis.com/auth/spreadsheets https://www.googleapis.com/auth/calendar https://www.googleapis.com/auth/drive.readonly"
        
        # For login, we'll create a temporary state and get user info from Google
        import uuid
        temp_state = str(uuid.uuid4())
        
        params = {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": scope,
            "access_type": "offline",
            "prompt": "consent",
            "include_granted_scopes": "true",
            "state": temp_state
        }
        
        url = f"https://accounts.google.com/o/oauth2/v2/auth?{urllib.parse.urlencode(params)}"
        return RedirectResponse(url=url)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": str(e), "trace": traceback.format_exc()})

@app.get("/api/oauth/google/authorize")
async def google_authorize(request: Request, user: dict = Depends(get_current_user)):
    try:
        client_id = os.environ.get("GOOGLE_CLIENT_ID")
        # Dynamic Redirect URI based on request
        host = request.headers.get("host", "localhost:8027")
        scheme = "https" if "ngrok" in host else "http"
        redirect_uri = f"{scheme}://{host}/api/oauth/google/callback"
        print(f"[OAuth] Using Dynamic Redirect URI: {redirect_uri}")
        
        if not client_id:
            raise HTTPException(status_code=500, detail="Missing GOOGLE_CLIENT_ID in env")

        # Include openid/email/profile for consistency (though not strictly needed for re-auth)
        scope = "openid email profile https://www.googleapis.com/auth/spreadsheets https://www.googleapis.com/auth/calendar https://www.googleapis.com/auth/drive.readonly"
        
        # State carries user_id to callback
        state = user.id
        
        params = {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": scope,
            "access_type": "offline",
            "prompt": "consent",
            "include_granted_scopes": "true",
            "state": state
        }
        
        url = f"https://accounts.google.com/o/oauth2/v2/auth?{urllib.parse.urlencode(params)}"
        return {"url": url}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": str(e), "trace": traceback.format_exc()})

@app.get("/api/oauth/google/callback")
async def google_callback(request: Request, code: str = None, state: str = None, error: str = None):
    # Log all incoming parameters for debugging
    print(f"[OAuth Callback] Received State: {state}, Code: {code}, Error: {error}")
    print(f"[OAuth Callback] Request URL: {request.url}")
    print(f"[OAuth Callback] Request headers host: {request.headers.get('host')}")
    print(f"[OAuth Callback] Request scheme: {request.url.scheme}")

    if error:
        return HTMLResponse(f"<h1>OAuth Error</h1><p>{error}</p>")
    if not code or not state:
        return HTMLResponse("<h1>OAuth Error</h1><p>Missing code or state parameter.</p>")

    client_id = os.environ.get("GOOGLE_CLIENT_ID")
    client_secret = os.environ.get("GOOGLE_CLIENT_SECRET")
    
    # Dynamically determine redirect_uri from the request to match what was used in authorization
    # Use the actual request URL to ensure exact match
    redirect_uri = str(request.url).split('?')[0]  # Remove query parameters to get base URL
    
    print(f"[OAuth Callback] Using redirect_uri (from request URL): {redirect_uri}")
    
    if not client_id or not client_secret:
         return HTMLResponse("<h1>Error: Missing Config</h1>")

    token_url = "https://oauth2.googleapis.com/token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": redirect_uri
    }
    
    print(f"[OAuth Callback] Token exchange payload (without secret): client_id={client_id[:10] if client_id else 'None'}..., redirect_uri={redirect_uri}")
    
    try:
        res = requests.post(token_url, data=payload)
        tokens = res.json()
        
        if "error" in tokens:
            error_msg = tokens.get('error_description', tokens.get('error', 'Unknown error'))
            print(f"[OAuth] Google Token Exchange Error: {tokens}")
            print(f"[OAuth] Status Code: {res.status_code}")
            print(f"[OAuth] Full Response: {res.text}")
            return HTMLResponse(f"<h1>Google Error: {error_msg}</h1><p>Error: {tokens.get('error')}</p><p>Make sure the redirect URI is registered in Google Cloud Console.</p>")
            
        refresh_token = tokens.get("refresh_token")
        access_token = tokens.get("access_token")
        
        print(f"[OAuth] Token Exchange Success. Refresh Token Present: {bool(refresh_token)}")
        
        if not refresh_token:
             # This happens if user re-approves without 'prompt=consent' or if we already have it.
             # Since we force prompt=consent, this is weird.
             print(f"[OAuth] WARNING: No Refresh Token received! Response: {tokens}")
             return HTMLResponse(f"<h1>Error: No Refresh Token.</h1><p>Google Response: {tokens}</p><p>Please Revoke Access in your Google Account Settings and try again.</p>")

        # Check if this is login flow (UUID state) or authorization flow (user_id state)
        import uuid
        try:
            # Try to parse state as UUID - if successful, it's login flow
            uuid.UUID(state)
            is_login_flow = True
        except:
            is_login_flow = False

        from backend.supabase_client import supabase_adapter
        
        if is_login_flow:
            # LOGIN FLOW: Get user info from Google and create Supabase user
            userinfo_url = "https://www.googleapis.com/oauth2/v2/userinfo"
            headers = {"Authorization": f"Bearer {access_token}"}
            userinfo_res = requests.get(userinfo_url, headers=headers)
            user_info = userinfo_res.json()
            
            email = user_info.get("email")
            name = user_info.get("name")
            
            if not email:
                return HTMLResponse("<h1>Error: Could not get email from Google</h1>")
            
            # Create or get Supabase user
            user_id = None
            
            # First, try to lookup existing user
            user_id = supabase_adapter.get_user_id_by_email(email)
            
            if not user_id:
                print(f"[OAuth Login] User not found, creating new user for email: {email}")
                # Create user via admin API
                try:
                    from supabase import create_client
                    admin_client = create_client(
                        os.environ.get("SUPABASE_URL"),
                        os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
                    )
                    
                    # Create user via admin API
                    create_response = admin_client.auth.admin.create_user({
                        "email": email,
                        "email_confirm": True,  # Auto-confirm email for OAuth users
                        "user_metadata": {
                            "full_name": name,
                            "provider": "google",
                            "oauth_login": True
                        }
                    })
                    
                    if create_response and hasattr(create_response, 'user'):
                        user_id = create_response.user.id
                        print(f"[OAuth Login] Created new user via admin API: {user_id}")
                        
                        # Create Twilio subaccount for new user
                        try:
                            from backend.twilio_subaccount import create_twilio_subaccount
                            success, error = create_twilio_subaccount(user_id, supabase_adapter)
                            if success:
                                print(f"[OAuth Login] Twilio subaccount created for user {user_id}")
                            else:
                                print(f"[OAuth Login] WARNING: Failed to create Twilio subaccount: {error}")
                                # Don't fail user creation, but log the error
                        except Exception as twilio_error:
                            print(f"[OAuth Login] ERROR creating Twilio subaccount: {twilio_error}")
                            import traceback
                            traceback.print_exc()
                            # Don't fail user creation, but log the error
                    else:
                        print(f"[OAuth Login] Admin create returned unexpected response: {create_response}")
                except Exception as create_error:
                    print(f"[OAuth Login] Admin create failed: {create_error}")
                    import traceback
                    traceback.print_exc()
                    
                    # Fallback: try sign_up (might work if user doesn't exist)
                    try:
                        sign_up_response = supabase.auth.sign_up({
                            "email": email,
                            "password": "oauth_user_" + state + "_" + str(int(time.time())),  # Unique dummy password
                            "options": {
                                "data": {
                                    "full_name": name,
                                    "provider": "google",
                                    "oauth_login": True
                                }
                            }
                        })
                        if sign_up_response and hasattr(sign_up_response, 'user'):
                            user_id = sign_up_response.user.id
                            print(f"[OAuth Login] Created user via sign_up: {user_id}")
                            
                            # Create Twilio subaccount for new user
                            try:
                                from backend.twilio_subaccount import create_twilio_subaccount
                                success, error = create_twilio_subaccount(user_id, supabase_adapter)
                                if success:
                                    print(f"[OAuth Login] Twilio subaccount created for user {user_id}")
                                else:
                                    print(f"[OAuth Login] WARNING: Failed to create Twilio subaccount: {error}")
                            except Exception as twilio_error:
                                print(f"[OAuth Login] ERROR creating Twilio subaccount: {twilio_error}")
                                import traceback
                                traceback.print_exc()
                    except Exception as sign_up_error:
                        print(f"[OAuth Login] Sign up also failed: {sign_up_error}")
            
            # Final lookup attempt if still no user_id
            if not user_id:
                user_id = supabase_adapter.get_user_id_by_email(email)
            
            if not user_id:
                import traceback
                print(f"[OAuth Login] CRITICAL: Could not find or create user for {email}")
                traceback.print_exc()
                return HTMLResponse(f"<h1>Error: Could not find or create user in Supabase</h1><p>Email: {email}</p><p>Check server logs for details.</p>")
            
            # Ensure Twilio subaccount exists (if user was found, not created)
            # This handles cases where user exists but subaccount wasn't created
            try:
                from backend.twilio_subaccount import create_twilio_subaccount
                twilio_success, twilio_error = create_twilio_subaccount(user_id, supabase_adapter)
                if twilio_success:
                    print(f"[OAuth Login] Twilio subaccount verified/created for user {user_id}")
                elif twilio_error and "already exists" not in twilio_error.lower():
                    print(f"[OAuth Login] WARNING: Twilio subaccount issue: {twilio_error}")
            except Exception as twilio_error:
                print(f"[OAuth Login] ERROR checking Twilio subaccount: {twilio_error}")
                # Don't fail the login, but log the error
            
            print(f"[OAuth Login] Using user_id: {user_id} (type: {type(user_id)}) for email: {email}")
            print(f"[OAuth Login] Refresh token length: {len(refresh_token) if refresh_token else 0}")
            
            # Store Google refresh token with proper user_id
            # Ensure user_id is a string
            if not isinstance(user_id, str):
                user_id = str(user_id)
            
            success = supabase_adapter.store_google_refresh_token(user_id, refresh_token)
            if success:
                print(f"[OAuth Login] Token Saved to DB for user {user_id}")
                return HTMLResponse(f"<h1>Success! Google Connected. you may close this window.</h1><script>window.opener.postMessage({{'type': 'google-connected', 'user_id': '{user_id}', 'email': '{email}'}}, '*'); setTimeout(() => window.close(), 1000);</script>")
            else:
                print(f"[OAuth Login] Failed to save token. Check server logs above for Supabase error details.")
                return HTMLResponse("<h1>Error: Could not save token. Check server logs.</h1>")
        
        else:
            # AUTHORIZATION FLOW: state is user_id
            user_id = state
            
            # Save to DB
            success = supabase_adapter.store_google_refresh_token(user_id, refresh_token)
            if success:
                print(f"[OAuth Auth] Token Saved to DB for user {user_id}")
                return HTMLResponse(f"<h1>Success! Google Connected. you may close this window.</h1><script>window.opener.postMessage({{'type': 'google-connected', 'user_id': '{user_id}'}}, '*'); setTimeout(() => window.close(), 2000);</script>")
            else:
                print(f"[OAuth Auth] Failed to save token to DB.")
                return HTMLResponse("<h1>Database Error: Could not save token. Check server logs.</h1>")

    except Exception as e:
        print(f"[OAuth Callback] Exception: {e}")
        import traceback
        traceback.print_exc()
        return HTMLResponse(f"<h1>System Error: {e}</h1>")


# --- TWILIO SUBACCOUNT MANAGEMENT ---

@app.post("/api/admin/ensure-twilio-subaccount")
async def ensure_twilio_subaccount(user: dict = Depends(get_current_user)):
    """
    Admin endpoint to ensure Twilio subaccount exists for current user.
    Useful for recovery if subaccount creation was missed during signup.
    """
    try:
        from backend.twilio_subaccount import create_twilio_subaccount
        from backend.supabase_client import supabase_adapter
        
        user_id = user.id
        success, error = create_twilio_subaccount(user_id, supabase_adapter)
        
        if success:
            return {"status": "success", "message": "Twilio subaccount verified/created"}
        else:
            return JSONResponse(
                status_code=500,
                content={"status": "error", "message": error or "Failed to create Twilio subaccount"}
            )
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(e)}
        )



# --- WORKFLOW API ---

class WorkflowStep(BaseModel):
    type: str 
    operation_category: str | None = None
    update_mode: str | None = None
    mapping_mode: str | None = None
    sheet_id: str | None = None
    tab_name: str | None = None 
    range_a1: str | None = None
    lookup_config: dict | None = None
    column_mapping: dict | None = None
    read_config: dict | None = None
    routing_rules: list | None = None
    summary: str | None = None
    smart_instruction: str | None = None # NEW

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8027, log_level="warning", access_log=False)
