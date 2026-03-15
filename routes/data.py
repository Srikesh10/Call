"""
routes/data.py — Data endpoints for leads, inventory, and knowledge base.
"""
import logging
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from dependencies import get_current_user
from backend.supabase_client import supabase_adapter

logger = logging.getLogger("alora.data")

router = APIRouter(prefix="/api", tags=["Data"])


@router.get("/leads")
async def get_leads(user: dict = Depends(get_current_user)):
    """Get all leads for this user."""
    return supabase_adapter.get_leads(user.id)


@router.delete("/leads/{lead_id}")
async def delete_lead(lead_id: str, user: dict = Depends(get_current_user)):
    """Delete a lead."""
    try:
        supabase_adapter.delete_lead(user.id, lead_id)
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Failed to delete lead: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/inventory")
async def get_inventory():
    """Get inventory items (public for agent access)."""
    return supabase_adapter.get_inventory()


@router.post("/upload_inventory")
async def upload_inventory(file: UploadFile = File(...), user: dict = Depends(get_current_user)):
    """Upload an inventory CSV file."""
    try:
        import csv
        import io
        contents = await file.read()
        decoded = contents.decode("utf-8")
        reader = csv.DictReader(io.StringIO(decoded))
        rows = list(reader)
        supabase_adapter.upload_inventory(user.id, rows)
        return {"status": "success", "rows_imported": len(rows)}
    except Exception as e:
        logger.error(f"Failed to upload inventory: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/kb")
async def get_kb(user: dict = Depends(get_current_user)):
    """Get knowledge base entries."""
    return supabase_adapter.get_knowledge_base(user.id)


@router.post("/kb")
async def add_kb(payload: dict, user: dict = Depends(get_current_user)):
    """Add a knowledge base entry."""
    try:
        result = supabase_adapter.add_knowledge_base_entry(user.id, payload)
        return {"status": "success", "entry": result}
    except Exception as e:
        logger.error(f"Failed to add KB entry: {e}")
        raise HTTPException(status_code=500, detail=str(e))
