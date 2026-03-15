"""
routes/config.py — User configuration, calendar toggles, and integrations.
"""
import logging

from fastapi import APIRouter, Request, Depends
from fastapi.responses import JSONResponse

from dependencies import get_current_user

logger = logging.getLogger("alora.config")

router = APIRouter(prefix="/api", tags=["Config"])


@router.post("/config")
async def save_config(config: dict, user: dict = Depends(get_current_user)):
    """Save general app options for the user."""
    from backend.supabase_client import supabase_adapter

    success = supabase_adapter.save_app_options(user.id, config)

    if success:
        return {"status": "success"}
    return JSONResponse(status_code=500, content={"status": "error"})


@router.get("/config")
async def get_config(user: dict = Depends(get_current_user)):
    """Get general app options for the user."""
    from backend.supabase_client import supabase_adapter
    options = supabase_adapter.get_app_options(user.id)
    return options


@router.post("/save_calendar")
async def save_calendar(request: Request, user: dict = Depends(get_current_user)):
    """Saves the calendar toggle setting without affecting other options."""
    from backend.supabase_client import supabase_adapter

    body = await request.json()
    enable_calendar = body.get("enable_calendar", False)

    options = supabase_adapter.get_app_options(user.id)
    if options is None:
        options = {}

    options["enable_calendar"] = enable_calendar
    supabase_adapter.save_app_options(user.id, options)

    logger.info(f"Calendar toggle set to {enable_calendar} for user {user.id}")
    return {"success": True, "enable_calendar": enable_calendar}


@router.delete("/integrations/{rule_id}")
async def delete_integration(rule_id: str, user: dict = Depends(get_current_user)):
    """Delete an app integration rule."""
    from backend.supabase_client import supabase_adapter

    success = supabase_adapter.delete_app_integration(user.id, rule_id)

    if success:
        return {"status": "success"}
    return JSONResponse(status_code=500, content={"status": "error", "message": "Invalid index or save failed"})


@router.post("/generate_prompt")
async def generate_prompt(payload: dict, user: dict = Depends(get_current_user)):
    """Use Groq to generate a system prompt from a user goal."""
    import os
    user_goal = payload.get("goal", "")
    current_prompt = payload.get("current_prompt", "")

    if not user_goal:
        return JSONResponse(status_code=400, content={"status": "error", "message": "Goal is required"})

    try:
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
            max_tokens=1024,
        )

        generated = completion.choices[0].message.content
        return {"status": "success", "prompt": generated}

    except Exception as e:
        logger.error(f"Prompt generation failed: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})
