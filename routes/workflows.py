"""
routes/workflows.py — Workflow CRUD endpoints.

Handles: listing, getting, saving, and deleting automation workflows.
"""
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from dependencies import get_current_user
from backend.supabase_client import supabase_adapter

logger = logging.getLogger("alora.workflows")

router = APIRouter(prefix="/api/workflows", tags=["Workflows"])

# ── Pydantic Models ──────────────────────────────────────────────────────────

class WorkflowData(BaseModel):
    id: Optional[str] = None
    name: str = "Untitled Workflow"
    trigger_type: str = "call_ended"
    trigger_config: dict = {}
    steps: list = []
    is_active: bool = True

# ── Endpoints ────────────────────────────────────────────────────────────────

@router.get("")
async def list_workflows(user: dict = Depends(get_current_user)):
    """List all workflows for the user."""
    try:
        workflows = supabase_adapter.get_workflows(user.id)
        return {"status": "success", "workflows": workflows}
    except Exception as e:
        logger.error(f"Failed to list workflows: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})


@router.get("/{workflow_id}")
async def get_workflow(workflow_id: str, user: dict = Depends(get_current_user)):
    """Get a single workflow."""
    try:
        workflow = supabase_adapter.get_workflow_by_id(workflow_id)

        if not workflow:
            return JSONResponse(status_code=404, content={"status": "error", "message": "Workflow not found"})

        if workflow["user_id"] != user.id:
            return JSONResponse(status_code=403, content={"status": "error", "message": "Unauthorized"})

        return {"status": "success", "workflow": workflow}
    except Exception as e:
        logger.error(f"Failed to get workflow: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})


@router.post("/save")
async def save_workflow(payload: WorkflowData, user: dict = Depends(get_current_user)):
    """Save/Upsert a workflow."""
    try:
        data = payload.dict(exclude_none=True)
        result = supabase_adapter.save_workflow(user.id, data)

        if result:
            return {"status": "success", "workflow": result}
        else:
            return JSONResponse(status_code=500, content={"status": "error", "message": "Failed to save workflow"})

    except Exception as e:
        logger.error(f"Failed to save workflow: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})


@router.delete("/{workflow_id}")
async def delete_workflow(workflow_id: str, user: dict = Depends(get_current_user)):
    """Delete a workflow."""
    try:
        success = supabase_adapter.delete_workflow(user.id, workflow_id)

        if success:
            return {"status": "success"}
        else:
            return JSONResponse(status_code=500, content={"status": "error", "message": "Failed to delete workflow"})

    except Exception as e:
        logger.error(f"Failed to delete workflow: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})
