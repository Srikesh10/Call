"""
routes/sheets.py — Google Sheets and Calendar integration endpoints.
"""
import os
import logging

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

from dependencies import get_current_user

logger = logging.getLogger("alora.sheets")

router = APIRouter(prefix="/api", tags=["Sheets & Calendars"])


@router.get("/sheets")
async def list_sheets(user: dict = Depends(get_current_user)):
    """Fetches Google Sheets using credentials from the Vault."""
    from backend.supabase_client import supabase_adapter

    token_data = supabase_adapter.get_google_tokens(user.id)

    if not token_data or not token_data.get("refresh_token"):
        return {"files": [], "error": "Google Account not linked. Please re-login."}

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

        service = build('drive', 'v3', credentials=creds)
        results = service.files().list(
            q="mimeType='application/vnd.google-apps.spreadsheet'",
            pageSize=10, fields="nextPageToken, files(id, name)"
        ).execute()

        items = results.get('files', [])
        return {"files": [{"id": f["id"], "name": f["name"]} for f in items]}

    except Exception as e:
        logger.error(f"Error listing sheets: {e}")
        return {"files": [], "error": str(e)}


@router.get("/calendars")
async def list_calendars(user: dict = Depends(get_current_user)):
    return {"status": "ok", "message": "Calendar integration ready"}


@router.post("/sheets/select")
async def select_sheet(payload: dict):
    return {"status": "success", "message": "Sheet selected"}


@router.get("/sheets/{sheet_id}/data")
async def get_sheet_data(sheet_id: str, user: dict = Depends(get_current_user)):
    """Fetch data from a specific Google Sheet for column selection and import."""
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

        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range='A1:ZZ1000'
        ).execute()

        values = result.get('values', [])

        if not values:
            return {"rows": []}

        headers = values[0]
        rows = []
        for row_data in values[1:]:
            row_dict = {}
            for i, header in enumerate(headers):
                row_dict[header] = row_data[i] if i < len(row_data) else ""
            rows.append(row_dict)

        return {"rows": rows, "headers": headers}

    except Exception as e:
        logger.error(f"Error fetching sheet data: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch sheet data: {str(e)}")


@router.post("/calendars/select")
async def select_calendar(payload: dict):
    return {"status": "error", "message": "Disabled"}


@router.get("/sheets/{sheet_id}/columns")
async def get_sheet_columns(sheet_id: str, user: dict = Depends(get_current_user)):
    """Fetch columns from the first sheet of a Google Spreadsheet."""
    try:
        from automation_engine import get_google_access_token, read_google_sheets_structure
        from backend.supabase_client import supabase_adapter

        tokens = supabase_adapter.get_google_tokens(user.id)
        if not tokens:
            return JSONResponse(status_code=400, content={"error": "Google not connected"})

        access_token = await get_google_access_token(tokens)
        if not access_token:
            return JSONResponse(status_code=401, content={"error": "Token refresh failed"})

        structure = await read_google_sheets_structure(access_token, sheet_id)

        if not structure or "sheets" not in structure:
            return {"columns": [], "sheet_name": "Sheet1"}

        first_sheet = structure["sheets"][0]
        sheet_name = first_sheet.get("properties", {}).get("title", "Sheet1")

        grid_data = first_sheet.get("data", [{}])
        row_data = grid_data[0].get("rowData", [{}]) if grid_data else [{}]
        header_values = row_data[0].get("values", []) if row_data else []

        columns = []
        for cell in header_values:
            val = cell.get("formattedValue", "")
            if val:
                columns.append(val)

        return {"columns": columns, "sheet_name": sheet_name}

    except Exception as e:
        logger.error(f"Error fetching sheet columns: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})
