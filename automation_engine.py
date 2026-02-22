import os
import json as json_lib
import asyncio
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from groq import AsyncGroq
import requests

# Init Groq
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
client = AsyncGroq(api_key=GROQ_API_KEY)

# Global Instance
from backend.supabase_client import supabase_adapter


# Global Cache for Schemas
SCHEMA_CACHE = {}

async def load_user_automation_data(user_id: str):
    """
    Load all user data from Supabase for automation processing
    Includes: app_options, app_integrations, Google tokens, sheets data
    """
    print(f"[AUTO] ===== LOADING AUTOMATION DATA =====")

    print(f"[AUTO] User ID: {user_id}")
    
    # 1. Load app_options (system prompt, settings)
    options = supabase_adapter.get_app_options(user_id)
    print(f"[AUTO] App Options loaded: {bool(options)}")
    if options:
        print(f"[AUTO] - Business Name: {options.get('business_name') or 'N/A'}")
        
        sys_prompt = options.get('system_instruction')
        display_prompt = sys_prompt[:50] + "..." if sys_prompt else "N/A"
        print(f"[AUTO] - System Prompt: {display_prompt}")
        
        print(f"[AUTO] - Calendar Enabled: {options.get('enable_calendar', False)}")
    
    # 2. Legacy rules skipped (per user request)
    automation_rules_raw = []

    
    # 3. Load Google tokens for API access
    google_tokens = supabase_adapter.get_google_tokens(user_id)
    print(f"[AUTO] Google tokens available: {bool(google_tokens)}")
    if google_tokens:
        print(f"[AUTO] - Has refresh token: {bool(google_tokens.get('refresh_token'))}")
    
    # Legacy Integrations Table skipped (per user request)
    rules = []


    # 4.5 Load Workflows from app_workflows (NEW)
    workflows = supabase_adapter.get_workflows(user_id)
    print(f"[AUTO] Loaded {len(workflows)} Workflows from DB")
    
    for wf in workflows:
        if not wf.get("is_active", True):
            continue
            
        rule = {
            "service": "workflow_engine",
            "id": wf.get("id"),
            "steps": wf.get("steps", []),
            "trigger_type": wf.get("trigger_type"),
            "trigger_config": wf.get("trigger_config", {}),
            "is_active": wf.get("is_active")
        }
        rules.append(rule)
        print(f"[AUTO] - Active Workflow: {wf.get('name')} (ID: {wf.get('id')})")

    # Fallback to legacy
    if not rules and automation_rules_raw:
        print("[AUTO] No integrations found, using legacy app_options rules")
        for rule in automation_rules_raw:
             if not rule.get("is_active", True): continue
             rules.append(rule)
    
    # 5. Compile complete automation data
    automation_data = {
        "user_id": user_id,
        "options": options or {},
        "rules": rules,
        "google_tokens": google_tokens,
        "system_prompt": options.get("system_instruction", "") if options else "",
        "outbound_system_prompt": options.get("outbound_system_instruction", "") if options else "",
        "business_name": options.get("business_name", "") if options else "",
        "supported_languages": options.get("supported_languages", ["en"]) if options else ["en"],
        "enable_calendar": options.get("enable_calendar", False) if options else False,
        "automation_rules": options.get("automation_rules", []) if options else []
    }
    
    print(f"[AUTO] ===== AUTOMATION DATA SUMMARY =====")
    print(f"[AUTO] - Business: {automation_data['business_name']}")
    print(f"[AUTO] - Active Rules: {len(automation_data['rules'])}")
    print(f"[AUTO] - Calendar: {automation_data['enable_calendar']}")
    print(f"[AUTO] - Languages: {automation_data['supported_languages']}")
    print(f"[AUTO] ======================================")
    
    return automation_data

async def process_call_background(call_id: int, user_id: str):
    """
    Process call automation in background
    Loads all user data from Supabase and performs post-call analysis
    """
    print(f"[AUTO] Starting Analysis for Call {call_id} (User ID: {user_id})")
    
    # 1. Load all user automation data from Supabase
    automation_data = await load_user_automation_data(user_id)
    
    if not automation_data:
        print("[AUTO] Failed to load automation data")
        return
    
    # 2. Fetch call from Supabase
    from backend.supabase_client import supabase_adapter
    call_data = supabase_adapter.get_call_by_id(call_id)
    
    if not call_data:
        print("[AUTO] Call not found.")
        return

    transcript = call_data.get('transcript', '')
    if not transcript:
        print("[AUTO] No transcript found.")
        return
    # --- METADATA EXTRACTION ---
    call_type = 'inbound'
    call_sid = None
    phone_number = None
    
    if call_data.get('automation_metadata'):
        meta = call_data.get('automation_metadata')
        if isinstance(meta, str):
            try:
                meta = json_lib.loads(meta)
            except: meta = {}
        if isinstance(meta, dict):
            call_type = meta.get('call_type', 'inbound')
            call_sid = meta.get('call_sid')
            phone_number = meta.get('phone_number')
            
    # --- OUTBOUND VERIFICATION ---
    if call_type == 'inbound' and call_sid:
        try:
             from backend.supabase_client import supabase_adapter
             if supabase_adapter.check_is_outbound_call(call_sid):
                 print(f"[AUTO] FOUND in outbound_calls table! Correcting CallType to 'outbound'")
                 call_type = 'outbound'
        except Exception as e:
            print(f"[AUTO] DB Verification failed: {e}")

    # --- PROMPT RESOLUTION ---
    # STRICT implementation of user request:
    # "access the system_instruction in app_options if its inbound calling and for outbound calling acess the outbound_system_prompt"
    
    # Check both keys for Inbound to be safe (User mentioned "is at system_prompt")
    opts = automation_data.get('options', {})
    inbound_base = opts.get('system_instruction') or opts.get('system_prompt', '')
    
    outbound_base = automation_data.get('outbound_system_prompt', '')
    
    # Force usage of settings based on verified call_type
    if call_type == 'outbound':
        print(f"[AUTO] 🚀 Call Type verified as OUTBOUND. Forcing usage of Outbound System Prompt from Settings.")
        automation_data['system_prompt'] = outbound_base
    else:
        print(f"[AUTO] Call Type verified as INBOUND. Using Inbound System Instruction from Settings.")
        automation_data['system_prompt'] = inbound_base
        
    # (Optional) If we ever need to support Workflow overrides again, we can re-enable the comparison logic here.
    # But for now, correctness of the base persona is the priority.
    
    # Prepend Mode indicator for analysis extraction context
    # Only if prompt exists
    if automation_data.get('system_prompt'):
         pass # Logic moved to next block in original file, just ensuring we set the key.
    # Prepend Mode indicator for analysis extraction context
    # No wrapper added, use clean prompt
    pass
    # -----------------------------

    # 3. Process each automation rule
    results = []
    print(f"[AUTO] ===== PROCESSING AUTOMATION RULES =====")
    print(f"[AUTO] Total rules to process: {len(automation_data['rules'])}")
    
    for i, rule in enumerate(automation_data['rules']):
        try:
            service = rule.get('service', '').strip() # Added .strip()
            
            # --- FILTERING LOGIC (RELAXED) ---
            # User Request: "remove this is outbound or inbound to skip... let both have it"
            # We will NOT skip rules. We just clean the service name.
            
            if service.endswith('_outbound'):
                rule['service'] = service.replace('_outbound', '')
                print(f"[DEBUG] Stripped suffix (Relaxed Mode) -> {rule['service']}")
            
            # REMOVED strict filtering blocks
            # if call_type == 'outbound': ...
            # else: ...

            print(f"[AUTO] --- Processing Rule {i+1}/{len(automation_data['rules'])} ---")
            print(f"[AUTO] Service: {rule['service']} (Original: {service})")
            if 'resource_name' in rule:
                print(f"[AUTO] Resource: {rule['resource_name']}")
            if 'resource_id' in rule:
                print(f"[AUTO] Resource ID: {rule['resource_id']}")
            print(f"[AUTO] Instruction: {rule.get('instruction', '')[:100]}...")
            
            # --- WORKFLOW ENGINE DISPATCH ---
            if rule['service'] == 'workflow_engine':
                # Check Run-On Trigger Filtering
                trigger_config = rule.get('trigger_config', {})
                run_on = trigger_config.get('run_on', 'any')
                
                if run_on != 'any' and run_on != call_type:
                    print(f"[AUTO] ⏭️ Skipping workflow '{rule.get('id')}' - Configured for {run_on} but this is {call_type}")
                    continue

                print("[AUTO] Workflow Engine Rule Detected - Executing Workflow Logic")
                await execute_workflow(transcript, rule, automation_data, phone_number, call_id)
                continue
            else:
                print(f"[AUTO] Skipping legacy service: {rule['service']}")
                continue

                
        except Exception as e:
            print(f"[AUTO] ERROR processing rule {rule['service']}: {e}")
            import traceback
            traceback.print_exc()
            results.append({"error": str(e), "rule": rule})
    
    print(f"[AUTO] ===== RULE PROCESSING COMPLETE =====")
    print(f"[AUTO] Total results: {len(results)}")

def evaluate_single_condition(actual_val, operator, target_val):
    """Evaluates a single condition against a value, supporting multiple types."""
    import re
    from datetime import datetime

    if actual_val is None:
        # Specialized handling for None, will not be logged by the new print statement
        # as it returns early. This is consistent with the provided edit.
        return operator in ["is_empty", "is_not_empty"] 

    actual_str = str(actual_val).strip()
    target_str = str(target_val).strip()

    # Log the result
    res = False
    # Refactoring for cleaner return with logging
    if operator == "==": res = actual_str.lower() == target_str.lower()
    elif operator == "!=": res = actual_str.lower() != target_str.lower()
    elif operator == "contains": res = target_str.lower() in actual_str.lower()
    elif operator == "not_contains": res = target_str.lower() not in actual_str.lower()
    elif operator == "starts_with": res = actual_str.lower().startswith(target_str.lower())
    elif operator == "ends_with": res = actual_str.lower().endswith(target_str.lower())
    elif operator == "is_empty": res = not actual_str
    elif operator == "is_not_empty": res = bool(actual_str)
    elif operator == "regex":
        try: res = bool(re.search(target_str, actual_str, re.IGNORECASE))
        except: res = False
    elif operator == "not_regex":
        try: res = not bool(re.search(target_str, actual_str, re.IGNORECASE))
        except: res = False
    # --- NUMBER CONDITIONS ---
    elif operator in [">", ">=", "<", "<=", "num_==", "num_!="]:
        try:
            f_actual = float(actual_str.replace("$", "").replace(",", ""))
            f_target = float(target_str.replace("$", "").replace(",", ""))
            if operator == ">": res = f_actual > f_target
            elif operator == ">=": res = f_actual >= f_target
            elif operator == "<": res = f_actual < f_target
            elif operator == "<=": res = f_actual <= f_target
            elif operator == "num_==": res = f_actual == f_target
            elif operator == "num_!=": res = f_actual != f_target
        except: pass
    elif operator == "is_true": res = actual_str.lower() in ["true", "yes", "1", "y"]
    elif operator == "is_false": res = actual_str.lower() in ["false", "no", "0", "n"]
    elif operator in ["after", "before", "date_=="]:
        try:
            d_actual = datetime.fromisoformat(actual_str.replace('Z', '+00:00'))
            d_target = datetime.fromisoformat(target_str.replace('Z', '+00:00'))
            if operator == "after": res = d_actual > d_target
            elif operator == "before": res = d_actual < d_target
            elif operator == "date_==": res = d_actual == d_target
        except: pass
    elif operator in ["arr_contains", "arr_not_contains", "length_>", "length_<"]:
        try:
            import json
            arr = []
            if isinstance(actual_val, list): arr = actual_val
            else: arr = json.loads(actual_str) if actual_str.startswith("[") else []
            if operator == "arr_contains": res = any(str(item).lower() == target_str.lower() for item in arr)
            elif operator == "arr_not_contains": res = not any(str(item).lower() == target_str.lower() for item in arr)
            elif operator == "length_>": res = len(arr) > float(target_val)
            elif operator == "length_<": res = len(arr) < float(target_val)
        except: pass

    print(f"[DEBUG] Evaluation: Field='{actual_val}' {operator} '{target_val}' -> {res}")
    return res

def evaluate_condition_group(group, context):
    """Recursively evaluates a group of conditions or nested groups."""
    logic = group.get("logic", "AND").upper()
    conditions = group.get("conditions", [])
    
    if not conditions: return True
    
    results = []
    for item in conditions:
        if "logic" in item: # Nested Group
            results.append(evaluate_condition_group(item, context))
        else: # Single Condition
            field = item.get("field")
            op = item.get("operator")
            val = item.get("value")
            
            actual_val = context.get(field)
            results.append(evaluate_single_condition(actual_val, op, val))
            
    if logic == "OR":
        return any(results)
    return all(results)


async def decide_smart_action(transcript: str, headers: list, instruction: str, call_metadata: dict = None) -> dict:
    """
    Uses LLM to decide the best course of action based on the instruction and available headers.
    Returns a JSON object with:
    - action: 'update', 'append', 'skip'
    - lookup_column: str (if update)
    - lookup_value_instruction: str (if update)
    - no_match_action: 'append', 'notify', 'skip'
    - data_extraction_instruction: str (for the next step)
    - update_columns: list (optional)
    """
    try:
        print("[SMART AGENT] 🤔 Analyzing instruction and headers...")
        
        # Prepare Metadata Context
        metadata_context = ""
        if call_metadata:
            import json
            metadata_str = json.dumps(call_metadata, indent=2)
            metadata_context = f"- Call Metadata Reference: {metadata_str}"
        
        prompt = f"""
You are an intelligent workflow automation architect.
Your goal is to interpret a user's natural language instruction and deciding how to modify a Google Sheet.

CONTEXT:
- Available Columns: {json_lib.dumps(headers)}
- User Instruction: "{instruction}"
- Transcript Excerpt: "{transcript[:500]}..." (Use this to understand what data might be available)
{metadata_context}

DECISION RULES:
1. **UPDATE (Specific Row)**: If the instruction implies checking for an existing record (e.g., "check if exists", "update status", "find user", "update lead"), choose action="update".
   - Identify the column that SEMANTICALLY matches the lookup intent (e.g., if searching for "who is calling", look for "Phone", "Mobile", or "Contact").
   - Pick the most relevant column from the 'Available Columns' list. If you are unsure, pick the most generic one (the backend has a multi-column fallback for phone numbers).
   - **STRICT RULE**: Your `lookup_value_instruction` MUST strictly follow the user's logic (e.g., "Use 'phone_number' from Call Metadata" if they mention the caller's ID/number).
   - For `lookup_value_instruction`, be precise (e.g., "Extract the user's phone number").
   - **CRITICAL**: If the user says "update status", DO NOT update every column. Only update the 'Status' column.

2. **APPEND (New Row)**: If the instruction implies just adding a new record (e.g., "add lead", "log call", "create new entry"), choose action="append".

3. **SKIP**: If the instruction is conditional and the condition isn't met (e.g., "only add if interested"), and the transcript shows they aren't interested, choose action="skip".

OUTPUT FORMAT:
Return ONLY valid JSON with this structure:
{{
  "action": "update" | "append" | "skip",
  "lookup_column": "ColumnName" (or null if append),
  "lookup_value_instruction": "Instruction to extract the lookup value" (or null if append),
  "no_match_action": "append" | "skip" | "notify",
  "update_columns": ["Column1", "Column2"] (Optional: List specific columns to update. If null, updates all mapped columns.),
  "reasoning": "Brief explanation of your decision"
}}
"""
        completion = await client.chat.completions.create(
            messages=[
                {"role": "system", "content": "You are a precise automation architect. Output ONLY JSON."},
                {"role": "user", "content": prompt}
            ],
            model="llama-3.3-70b-versatile",
            temperature=0.0,
            response_format={"type": "json_object"}
        )
        
        result = json_lib.loads(completion.choices[0].message.content)
        print(f"[SMART AGENT] 💡 Decision: {result.get('action').upper()} via {result.get('lookup_column') or 'N/A'}")
        return result

    except Exception as e:
        print(f"[SMART AGENT] Error in decision: {e}")
        # Fail safe
        return {"action": "append", "reasoning": "Error fallback"}

async def execute_workflow(transcript: str, rule: dict, automation_data: dict, phone_number: str = None, call_id: int = None):
    """
    Execute a multi-step workflow defined in the 'workflow_engine' rule.
    """
    try:
        print("[WORKFLOW] Starting Workflow Execution...")
        steps = rule.get("steps", [])
        
        if not steps:
            print("[WORKFLOW] No steps found in workflow configuration.")
            return

        workflow_context = {} # Shared context across steps

        for step in steps:
            step_type = step.get("type")
            print(f"[WORKFLOW] Executing Step: {step_type}")
            
            if step_type == 'sheets':
                try:
                    category = step.get("operation_category", "write") # Kept for backward compat
                    sheet_id = step.get("sheet_id")
                    tab_name = step.get("tab_name")
                    
                    if not sheet_id:
                        print("[WORKFLOW] ERROR: No sheet_id found")
                        continue

                    access_token = await get_google_access_token(automation_data['google_tokens'])
                    if not access_token:
                        print("[WORKFLOW] Failed to get Access Token")
                        continue

                    # --- SMART SHEETS LOGIC ---
                    smart_instruction = step.get("smart_instruction")
                    
                    # Default modes (fallback)
                    update_mode = step.get("update_mode", "upsert")
                    mapping_mode = step.get("mapping_mode", "manual")
                    column_mapping = step.get("column_mapping", {})
                    column_mapping = step.get("column_mapping", {})
                    lookup_config = step.get("lookup_config", {})
                    routing_rules = step.get("routing_rules", []) # Initialize here
                    
                    # 1. Resolve Tab Name
                    current_tab = tab_name
                    if not current_tab:
                         try:
                             meta_url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}"
                             async with httpx.AsyncClient() as h_client:
                                 resp = await h_client.get(meta_url, headers={"Authorization": f"Bearer {access_token}"})
                                 current_tab = resp.json().get("sheets", [{}])[0].get("properties", {}).get("title", "Sheet1")
                         except: current_tab = "Sheet1"

                    # 2. Fetch Headers (Crucial for Smart Mode)
                    headers = await fetch_google_sheets_headers(sheet_id, access_token, tab_name=current_tab)
                    if not headers:
                        print("[WORKFLOW] Error: Could not fetch headers. Skipping.")
                        continue

                    # 3. Decision Phase (Smart Mode)
                    if smart_instruction:
                        print(f"[WORKFLOW] 🧠 Smart Mode: '{smart_instruction}'")
                        
                        # Prepare Metadata for LLM
                        call_metadata = {
                            "phone_number": phone_number,
                            "call_id": call_id,
                            "call_type": "inbound"  # Default, can be refined if passed
                        }
                        
                        decision = await decide_smart_action(transcript, headers, smart_instruction, call_metadata=call_metadata)
                        
                        action = decision.get("action", "append")
                        
                        if action == "skip":
                            print("[WORKFLOW] Smart Agent decided to SKIP this step.")
                            continue
                            
                        # Configure based on decision
                        if action == "update":
                            update_mode = "update"
                            lookup_col = decision.get("lookup_column")
                            lookup_instr = decision.get("lookup_value_instruction")
                            
                            if lookup_col and lookup_instr:
                                print(f"[WORKFLOW] Smart Update: Looking up '{lookup_col}' using '{lookup_instr}'")
                                # Pass metadata as context string for extraction
                                meta_ctx = f"Call Metadata: {json_lib.dumps(call_metadata)}"
                                lookup_config = {"column": lookup_col, "value_instruction": lookup_instr, "no_match_action": decision.get("no_match_action", "append"), "context": meta_ctx}
                            else:
                                print("[WORKFLOW] Smart Update failed to provide lookup details. Falling back to Append.")
                                update_mode = "append"
                        else:
                            update_mode = "append"
                        
                        # Force Auto-Mapping for Smart Mode
                        mapping_mode = "auto" 

                    # --- CONTEXT INJECTION PREP ---
                    execution_logic_context = "" # Initialize here to prevent NameError
                    existing_data_context = ""
                    cached_row_idx = None 
                    
                    if update_mode in ["update", "upsert"]:
                        print("[WORKFLOW] Checking for existing row...")
                        lookup_val = phone_number
                        val_instr = lookup_config.get("value_instruction")
                        
                        if val_instr:
                            # --- METADATA DIRECT ASSIGNMENT (FAST-TRACK) ---
                            if "Call Metadata" in val_instr or "phone_number" in val_instr:
                                if phone_number and phone_number != "Unknown":
                                    lookup_val = phone_number
                                    print(f"[WORKFLOW] 🎯 Smart Lookup: Using pre-extracted phone '{lookup_val}' directly.")
                                else:
                                    print(f"[WORKFLOW] ⚠️ Warning: Metadata lookup requested but phone_number is '{phone_number}'. Falling back to LLM.")
                                    lookup_val = await extract_single_value_with_llm(transcript, val_instr, context=f"Caller Phone: {phone_number}")
                            else:
                                # Standard LLM Extraction
                                try:
                                    ctx = lookup_config.get("context", "")
                                    extracted_val = await extract_single_value_with_llm(transcript, val_instr, context=ctx)
                                    # Validate Extraction
                                    if extracted_val and extracted_val not in ["NOT_FOUND", "None", ""]:
                                        lookup_val = extracted_val
                                        print(f"[WORKFLOW] 🧠 Smart Lookup extracted: '{lookup_val}'")
                                    else:
                                        print(f"[WORKFLOW] ⚠️ Smart Lookup failed (Got '{extracted_val}'). Falling back to Metadata Phone: '{phone_number}'")
                                        # Fallback to phone_number is already set by default
                                except Exception as e:
                                    print(f"[WORKFLOW] Error in Smart Lookup: {e}. Fallback to phone.")
                        
                        lookup_col = lookup_config.get("column")
                        print(f"[DEBUG] Lookup: Col='{lookup_col}', Val='{lookup_val}'")
                        
                        if lookup_col and lookup_val:
                            row_idx, row_data = await find_row_index_for_update(sheet_id, access_token, lookup_val, lookup_col, tab_name=current_tab, return_data=True)
                            cached_row_idx = row_idx
                            
                            if row_data:
                                ctx_list = [f"{h}: {row_data[i] if i < len(row_data) else ''}" for i, h in enumerate(headers)]
                                existing_data_context = "\nEXISTING_ROW_DATA:\n" + "\n".join(ctx_list)
                                print(f"[WORKFLOW] Found row {row_idx}. Context injected.")
                            else:
                                # Handle No Match
                                no_match = lookup_config.get("no_match_action", "append")
                                if no_match == "append":
                                    print("[WORKFLOW] Row not found. Switching to APPEND.")
                                    update_mode = "append" # Override to append
                                elif no_match == "skip":
                                    print("[WORKFLOW] Row not found. SKIPPING.")
                                    continue
                        else:
                             if smart_instruction:
                                 # If smart mode update failed specific lookup, fallback to append
                                 print("[WORKFLOW] Lookup details missing. Defaulting to APPEND.")
                                 update_mode = "append"

                    # --- GRANULAR UPDATE FILTERING (SMART MODE) ---
                    target_headers = headers
                    update_columns = decision.get("update_columns") if smart_instruction and 'decision' in locals() else None
                    
                    if update_mode == "update" and update_columns:
                        print(f"[WORKFLOW] 🎯 Granular Update: Targeting columns {update_columns}")
                        # Filter headers to only those requested
                        # Normalize for case-insensitive matching
                        valid_cols = [h for h in headers if h in update_columns or h.lower() in [c.lower() for c in update_columns]]
                        if valid_cols:
                            target_headers = valid_cols
                            print(f"[WORKFLOW] Filtered headers for update: {target_headers}")
                        else:
                            print(f"[WORKFLOW] ⚠️ Warning: Requested update_columns {update_columns} not found in sheet. Using all headers.")

                    if mapping_mode == "auto":
                        print(f"[WORKFLOW] Mode: Auto-Mapping. Fetching headers from {current_tab}...")
                        # Headers already fetched, just use target_headers
                        
                        if target_headers:
                            prompt = f"TRANSCRIPT:\n{transcript}\n"
                            prompt += existing_data_context
                            prompt += execution_logic_context
                            prompt += "\nIDENTIFY AND EXTRACT DATA FOR THESE COLUMNS:\n"
                            for h in target_headers:
                                prompt += f"- {h}\n"
                            prompt += "\nReturn ONLY valid JSON. Key=Column Name, Value=Extracted Data."
                            prompt += "\nNOTE: Be aware of the existing data and conditions. Ensure updates are consistent with the conversation."
                            
                            print(f"[DEBUG] Full LLM Prompt Context (Auto-Mapping):\n{prompt}\n")
                            completion = await client.chat.completions.create(
                                messages=[
                                    {"role": "system", "content": "You are a data extraction assistant. Output ONLY JSON."},
                                    {"role": "user", "content": prompt}
                                ],
                                model="llama-3.3-70b-versatile",
                                temperature=0.1,
                                response_format={"type": "json_object"}
                            )
                            raw_json = completion.choices[0].message.content
                            print(f"[DEBUG] Raw extraction JSON: {raw_json}")
                            extracted_data = json_lib.loads(raw_json)
                    elif column_mapping:
                        prompt = f"TRANSCRIPT:\n{transcript}\n"
                        prompt += existing_data_context
                        prompt += execution_logic_context
                        prompt += "\nEXTRACT COLUMNS:\n"
                        for col, instr in column_mapping.items():
                            prompt += f"- {col}: {instr}\n"
                        prompt += "\nReturn ONLY valid JSON. Key=Column Name, Value=Extracted Data."
                        prompt += "\nNOTE: Be aware of the existing data and conditions. Ensure updates are consistent with the conversation."
                        
                        print(f"[DEBUG] Full LLM Prompt Context (Column Mapping):\n{prompt}\n")
                        completion = await client.chat.completions.create(
                            messages=[
                                {"role": "system", "content": "You are a data extraction assistant. Output ONLY JSON."},
                                {"role": "user", "content": prompt}
                            ],
                            model="llama-3.3-70b-versatile",
                            temperature=0.1,
                            response_format={"type": "json_object"}
                        )
                        raw_json = completion.choices[0].message.content
                        print(f"[DEBUG] Raw extraction JSON: {raw_json}")
                        extracted_data = json_lib.loads(raw_json)
                    
                    print(f"[DEBUG] Final Extracted Data: {extracted_data}")

                    # --- EXECUTION GUARDS ---
                    should_execute = True
                    if routing_rules:
                        eval_context = {**workflow_context, **extracted_data}
                        should_execute = False
                        for rule in routing_rules:
                            is_match = False
                            if "logic" in rule or "conditions" in rule:
                                is_match = evaluate_condition_group(rule, eval_context)
                            else:
                                field = rule.get("field")
                                op = rule.get("operator", "==")
                                val = rule.get("value")
                                if field:
                                    is_match = evaluate_single_condition(eval_context.get(field), op, val)
                            if is_match:
                                should_execute = True
                                break
                    
                    if not should_execute:
                        print("[WORKFLOW] Conditions not met. Skipping Step.")
                        continue

                    # --- EXECUTION ---
                    if update_mode == "clear":
                        await clear_google_sheets_range(sheet_id, access_token, tab_name=current_tab, range_a1=step.get("range_a1"))
                    elif update_mode == "append":
                        await write_to_google_sheets(sheet_id, extracted_data, access_token, phone_number=phone_number, tab_name=current_tab)
                    else:
                        # Update or Upsert
                        row_idx = cached_row_idx
                        if row_idx is None:
                            # Fallback if not cached (shouldn't happen with current logic but for safety)
                            lookup_val = phone_number
                            val_instr = lookup_config.get("value_instruction")
                            if val_instr:
                                lookup_val = await extract_single_value_with_llm(transcript, val_instr)
                            lookup_col = lookup_config.get("column")
                            row_idx = await find_row_index_for_update(sheet_id, access_token, lookup_val, lookup_col, tab_name=current_tab)
                        
                        no_match_action = lookup_config.get("no_match_action", "nothing")

                        if row_idx:
                            print(f"[WORKFLOW] Updating existing row {row_idx} in sheet...")
                            await update_google_sheet_row(sheet_id, row_idx, extracted_data, access_token, phone_number=phone_number, tab_name=current_tab)
                            print(f"[WORKFLOW] ✅ Successfully updated row {row_idx}")
                        else:
                            if update_mode == "upsert" or no_match_action == "append":
                                print("[WORKFLOW] No match found. Performing UPSERT/APPEND...")
                                await write_to_google_sheets(sheet_id, extracted_data, access_token, phone_number=phone_number, tab_name=current_tab)
                                print("[WORKFLOW] ✅ Successfully appended new row")
                            elif no_match_action == "notify":
                                print(f"[WORKFLOW] ⚠️ No match found for {lookup_val}. Notifying admin.")
                            else:
                                print(f"[WORKFLOW] ⏭️ No match found for '{lookup_val}' in col '{lookup_col}'. Skipping (as per no_match_action).")
                except Exception as e:
                    print(f"[WORKFLOW] Error in Sheets Step: {e}")
                    import traceback
                    traceback.print_exc()

            elif step_type == 'calendar':
                print("[WORKFLOW] Delegating to Calendar Logic")
                await analyze_transcript_for_calendar(transcript, {}, automation_data)

    except Exception as e:
        print(f"[WORKFLOW] Execution Failed: {e}")
        import traceback
        traceback.print_exc()

    
    # 4. Store results
    # Initialize results if not defined (in case of error or no rules)
    if 'results' not in locals():
        results = []

    analysis_result = {
        "automation_results": results,
        "user_data": {
            "business_name": automation_data['business_name'],
            "system_prompt": automation_data['system_prompt'],
            "rules_processed": len(automation_data['rules'])
        }
    }
    
    supabase_adapter.update_call_analysis(call_id, "completed", json_lib.dumps(analysis_result))
    print(f"[AUTO] Analysis complete for Call {call_id}")

# --- SERVICE PROCESSING FUNCTIONS ---

# LEGACY SERVICE HANDLERS REMOVED




async def list_google_sheets(access_token: str) -> list:
    """List Google Sheets files from Drive"""
    import httpx
    try:
        url = "https://www.googleapis.com/drive/v3/files"
        # Query for mimeType = spreadsheet
        params = {
            "q": "mimeType='application/vnd.google-apps.spreadsheet' and trashed=false",
            "fields": "files(id, name)",
            "orderBy": "modifiedTime desc",
            "pageSize": 20
        }
        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                params=params,
                headers={"Authorization": f"Bearer {access_token}"}
            )
            if response.status_code == 200:
                return response.json().get("files", [])
            else:
                print(f"[AUTO] Error listing sheets: {response.text}")
                return []
    except Exception as e:
        print(f"[AUTO] Exception listing sheets: {e}")
        return []


async def read_google_sheets_structure(sheet_id: str, access_token: str) -> dict:
    """Read the structure of a Google Sheet to understand columns and data format"""
    import httpx
    
    try:
        # First get basic sheet metadata
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}"
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json"
                }
            )
            
            if response.status_code == 200:
                sheet_data = response.json()
                
                # Extract basic structure
                structure = {
                    "sheet_title": sheet_data.get("properties", {}).get("title", "Unknown"),
                    "sheets": []
                }
                
                for sheet in sheet_data.get("sheets", []):
                    sheet_title = sheet.get("properties", {}).get("title", "Sheet1")
                    numeric_sheet_id = sheet.get("properties", {}).get("sheetId", "0")  # Don't overwrite sheet_id param!
                    
                    sheet_info = {
                        "title": sheet_title,
                        "columns": [],
                        "sample_rows": []
                    }
                    
                    # Get column headers using A1 notation
                    range_name = f"'{sheet_title}'!A1:Z1"  # First row, columns A-Z
                    headers_url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{range_name}"
                    
                    print(f"[AUTO] Fetching headers from: {range_name}")
                    headers_response = await client.get(
                        headers_url,
                        headers={"Authorization": f"Bearer {access_token}"}
                    )
                    
                    print(f"[AUTO] Headers API status: {headers_response.status_code}")
                    if headers_response.status_code == 200:
                        headers_data = headers_response.json()
                        print(f"[AUTO] Headers response: {headers_data}")
                        values = headers_data.get("values", [])
                        if values:
                            headers = values[0]  # First row contains headers
                            sheet_info["columns"] = [str(h) for h in headers if h]  # Remove empty headers
                            print(f"[AUTO] Extracted columns: {sheet_info['columns']}")
                            
                            # Get sample data (next few rows)
                            sample_range = f"'{sheet_title}'!A2:Z10"  # Rows 2-10
                            print(f"[AUTO] Fetching sample data from: {sample_range}")
                            sample_response = await client.get(
                                f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{sample_range}",
                                headers={"Authorization": f"Bearer {access_token}"}
                            )
                            
                            print(f"[AUTO] Sample data API status: {sample_response.status_code}")
                            if sample_response.status_code == 200:
                                sample_data = sample_response.json()
                                print(f"[AUTO] Sample data response: {sample_data}")
                                sample_values = sample_data.get("values", [])
                                for row in sample_values[:5]:  # First 5 sample rows
                                    if row and any(cell.strip() if isinstance(cell, str) else str(cell).strip() for cell in row):  # Non-empty rows
                                        # Pad row to match column count
                                        padded_row = row + [''] * (len(headers) - len(row))
                                        sheet_info["sample_rows"].append(padded_row[:len(headers)])
                                print(f"[AUTO] Extracted {len(sheet_info['sample_rows'])} sample rows")
                            else:
                                print(f"[AUTO] Failed to fetch sample data: {sample_response.text}")
                        else:
                            print(f"[AUTO] WARNING: No header values found in range {range_name}")
                    else:
                        print(f"[AUTO] Failed to fetch headers: {headers_response.text}")
                    
                    structure["sheets"].append(sheet_info)
                
                return structure
            else:
                print(f"[AUTO] Error reading sheet structure: {response.status_code} - {response.text}")
                return {"error": "Failed to read sheet structure", "status_code": response.status_code}
                
    except Exception as e:
        print(f"[AUTO] Exception reading sheet structure: {e}")
        return {"error": str(e)}

# --- HELPER FUNCTIONS ---

async def get_google_access_token(google_tokens: dict) -> str:
    """Get fresh Google access token using refresh token"""
    print(f"[TOKEN] get_google_access_token called with: {list(google_tokens.keys()) if google_tokens else 'None'}")
    
    if not google_tokens or not google_tokens.get("refresh_token"):
        print("[TOKEN] ERROR: No refresh_token found in google_tokens")
        return None
    
    try:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        from auth_config import CLIENT_ID, CLIENT_SECRET, TOKEN_URI
        
        print(f"[TOKEN] Using refresh_token: {google_tokens['refresh_token'][:20]}...")
        
        creds = Credentials(
            token=None,
            refresh_token=google_tokens["refresh_token"],
            token_uri=TOKEN_URI,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/calendar", "https://www.googleapis.com/auth/drive.readonly"]
        )
        
        creds.refresh(Request())
        print(f"[TOKEN] Successfully obtained access_token: {creds.token[:20]}...")
        return creds.token
        
    except Exception as e:
        print(f"[TOKEN] ERROR refreshing Google token: {e}")
        import traceback
        traceback.print_exc()
        return None

# --- ANALYSIS FUNCTIONS ---

async def analyze_transcript_for_sheets(transcript: str, rule: dict, automation_data: dict, sheet_structure: dict = None) -> dict:
    """Analyze transcript for Google Sheets data extraction with sheet context"""
    
    # Build context about the sheet structure
    sheet_context = ""
    if sheet_structure and "sheets" in sheet_structure:
        for sheet in sheet_structure["sheets"]:
            sheet_context += f"\nSheet: {sheet['title']}\n"
            if sheet["columns"]:
                sheet_context += f"Columns: {', '.join(sheet['columns'])}\n"
            if sheet["sample_rows"]:
                sheet_context += "Sample data:\n"
                for i, row in enumerate(sheet["sample_rows"][:3]):  # Show first 3 sample rows
                    sheet_context += f"  Row {i+1}: {', '.join(row)}\n"
    
    target_column = rule.get('target_column')
    target_instruction = ""
    if target_column:
        target_instruction = f"""
        CRITICAL: The user wants to populate specifically the column '{target_column}'.
        You MUST extract data ONLY for '{target_column}' if available.
        Other columns should be null unless they are required context or mentioned in instruction.
        Focus heavily on extracting value for: {target_column}
        """

    system_prompt = f"""
    You are a data extraction expert for {automation_data['business_name']}.
    
    Analyze this call transcript and extract relevant data for the spreadsheet: {rule['resource_name']}
    
    SPREADSHEET STRUCTURE:
    {sheet_context}
    
    Instruction: {rule['instruction']}
    {target_instruction}
    
    CRITICAL: You must respond with ONLY a valid JSON object. No explanations, no markdown, no code blocks.
    
    GUIDELINES:
    1. Look at the column headers above and extract data that matches those columns
    2. Use the sample data to understand the format and type of data expected
    3. Only extract information that is explicitly mentioned in the transcript
    4. If no relevant data is found for a column, use null
    5. Return data as JSON object with keys matching the column headers exactly
    
    Example format: {{"Column1": "extracted_value", "Column2": null, "Column3": "extracted_value"}}
    
    Available columns: {sheet_structure.get('sheets', [{}])[0].get('columns', []) if sheet_structure and sheet_structure.get('sheets') else []}
    """
    
    try:
        response = await client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Transcript:\n{transcript}"}
            ],
            temperature=0.1,
            response_format={"type": "json_object"}  # Enforce JSON output
        )
        
        content = response.choices[0].message.content
        print(f"[AUTO] LLM Analysis: {content[:200]}...")
        
        # Parse JSON response
        try:
            import json
            parsed_data = json.loads(content)
            print(f"[AUTO] Successfully parsed JSON: {list(parsed_data.keys())}")
            return parsed_data
        except json.JSONDecodeError as e:
            print(f"[AUTO] JSON decode error: {e}")
            # Try to extract JSON from text if it's wrapped in markdown
            import re
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                try:
                    return json.loads(json_match.group())
                except:
                    pass
            
            print(f"[AUTO] Failed to parse JSON, returning raw analysis")
            return {"raw_analysis": content, "error": "JSON parsing failed"}
            
    except Exception as e:
        print(f"[AUTO] Error analyzing transcript for sheets: {e}")
        return {"error": str(e)}

async def extract_single_value_with_llm(transcript: str, instruction: str, context: str = "") -> str:
    """Uses LLM to extract a single string value from transcript based on instruction. Accepts optional context."""
    try:
        prompt = f"TRANSCRIPT:\n{transcript}\n"
        if context:
            prompt += f"\nWORKFLOW CONTEXT / METADATA:\n{context}\n"
        
        prompt += f"\nINSTRUCTION: {instruction}\n\n"
        prompt += "Guidelines:\n"
        prompt += "1. Extract the specific value requested.\n"
        prompt += "2. If the value is explicitly present in the 'WORKFLOW CONTEXT / METADATA', use it.\n"
        prompt += "3. Return ONLY the value string, no JSON, no quotes, no explanation.\n"
        prompt += "4. If not found in Transcript OR Context, return 'NOT_FOUND'."
        
        completion = await client.chat.completions.create(
            messages=[
                {"role": "system", "content": "You are a precise data extraction agent. Return only the value requested."},
                {"role": "user", "content": prompt}
            ],
            model="llama-3.3-70b-versatile",
            temperature=0.0
        )
        val = completion.choices[0].message.content.strip()
        # Clean up common LLM noise
        if val.startswith("'") or val.startswith('"'):
            val = val[1:-1]
        
        return None if val == "NOT_FOUND" or not val else val
    except Exception as e:
        print(f"[WORKFLOW] Error in extract_single_value: {e}")
        return None

def index_to_a1(index: int) -> str:
    """Converts a 0-based column index to A1 notation (A, B, ... Z, AA, AB, ...)."""
    result = ""
    while index >= 0:
        result = chr((index % 26) + ord('A')) + result
        index = (index // 26) - 1
    return result

async def fetch_google_sheets_headers(sheet_id: str, access_token: str, tab_name: str = None) -> list:
    """Fetch the first row of a Google Sheet to use as headers."""
    import httpx
    # Fetch up to 100 columns
    range_notation = f"'{tab_name}'!A1:CV1" if tab_name else "A1:CV1"
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{range_notation}"
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(url, headers={"Authorization": f"Bearer {access_token}"})
            data = resp.json()
            values = data.get("values", [])
            if values:
                return [str(h).strip() for h in values[0]]
            return []
        except Exception as e:
            print(f"[SHEETS] Error fetching headers: {e}")
            return []

async def fetch_google_sheets_rows(sheet_id: str, access_token: str, tab_name: str = None, range_a1: str = None) -> list:
    """Fetch rows from a sheet, optionally within a specific range or tab."""
    import httpx
    if not range_a1:
        range_a1 = "A:CV" # Default to all data
    
    range_notation = f"'{tab_name}'!{range_a1}" if tab_name else range_a1
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{range_notation}"
    
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(url, headers={"Authorization": f"Bearer {access_token}"})
            return resp.json().get("values", [])
        except Exception as e:
            print(f"[SHEETS] Error fetching rows: {e}")
            return []

async def find_row_index_for_update(sheet_id: str, access_token: str, lookup_value: str, lookup_col: str, tab_name: str = None, return_data: bool = False):
    """Finds the 1-based row index for a value. Supports fuzzy headers and multi-column phone fallback."""
    import httpx
    headers = await fetch_google_sheets_headers(sheet_id, access_token, tab_name=tab_name)
    if not headers: return None

    # 1. Improved Column Header Match (Primary Candidate)
    col_idx = -1
    clean_headers = [str(h).strip().lower() for h in headers]
    target_col = str(lookup_col).strip().lower() if lookup_col else None
    
    # Identify if the search value is likely a phone number
    search_str = str(lookup_value).strip()
    search_digits = "".join(filter(str.isdigit, search_str))
    is_phone_value = len(search_digits) >= 10
    phone_terms = ["phone", "mobile", "cell", "contact", "number", "tele", "whatsapp", "call"]

    if target_col:
        # A. Exact match
        if target_col in clean_headers:
            col_idx = clean_headers.index(target_col)
            print(f"[DEBUG] Header Match: Found EXACT '{target_col}' at index {col_idx}")
        else:
            # B. Substring match
            for i, ch in enumerate(clean_headers):
                if target_col in ch or ch in target_col:
                    col_idx = i
                    print(f"[DEBUG] Header Match: Found FUZZY '{target_col}' in '{headers[i]}' (Index: {i})")
                    break

    # C. Fallback for Phone Lookups (Initial Guess)
    if col_idx == -1 and is_phone_value:
        for i, ch in enumerate(clean_headers):
            if any(term in ch for term in phone_terms):
                col_idx = i
                print(f"[DEBUG] Header Match: FALLBACK to phone-like column '{headers[i]}' (Index: {i})")
                break

    if col_idx == -1 and not target_col:
         print(f"[DEBUG] Header Match: FAILED to find any suitable column. Headers: {headers}")
         return None

    # 2. MATCHING LOGIC (With Multi-Column Fallback)
    async with httpx.AsyncClient() as client:
        # Columns to try: [Primary Candidate] + [All other phone-like columns if is_phone_value]
        candidate_indices = []
        if col_idx != -1: candidate_indices.append(col_idx)
        
        if is_phone_value:
            for idx, h in enumerate(clean_headers):
                if idx not in candidate_indices and any(term in h for term in phone_terms):
                    candidate_indices.append(idx)
        
        if not candidate_indices:
             return None

        print(f"[DEBUG] Lookup candidates: {candidate_indices} (Headers: {[headers[i] for i in candidate_indices]})")

        for c_idx in candidate_indices:
            col_letter = index_to_a1(c_idx)
            range_notation = f"'{tab_name}'!{col_letter}:{col_letter}" if tab_name else f"{col_letter}:{col_letter}"
            url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{range_notation}"
            
            resp = await client.get(url, headers={"Authorization": f"Bearer {access_token}"})
            rows = resp.json().get("values", [])
            
            # Normalization
            cur_is_phone = any(term in clean_headers[c_idx] for term in phone_terms) or is_phone_value
            if cur_is_phone and len(search_digits) >= 10:
                match_val = search_digits[-10:]
                mode = "PHONE"
            else:
                match_val = search_str.lower()
                mode = "TEXT"

            print(f"[DEBUG] Column Check: '{headers[c_idx]}' | Mode: {mode} | Target: '{match_val}'")

            for i, row in enumerate(rows):
                if not row: continue
                raw_cell = str(row[0]).strip()
                
                if mode == "PHONE":
                    cell_digits = "".join(filter(str.isdigit, raw_cell))
                    if len(cell_digits) >= 10 and match_val == cell_digits[-10:]:
                        print(f"[DEBUG] ✅ Found Match (Phone): Row {i+1} in '{headers[c_idx]}'")
                        return await _get_return_data(client, sheet_id, access_token, tab_name, i + 1, return_data)
                else:
                    if match_val == raw_cell.replace(" ", "").replace("-", "").lower():
                         print(f"[DEBUG] ✅ Found Match (Text): Row {i+1} in '{headers[c_idx]}'")
                         return await _get_return_data(client, sheet_id, access_token, tab_name, i + 1, return_data)
        
    return (None, None) if return_data else None

async def _get_return_data(client, sheet_id, access_token, tab_name, row_idx, return_data):
    if not return_data: return row_idx
    range_row = f"'{tab_name}'!{row_idx}:{row_idx}" if tab_name else f"{row_idx}:{row_idx}"
    url_row = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{range_row}"
    resp_row = await client.get(url_row, headers={"Authorization": f"Bearer {access_token}"})
    row_data = resp_row.json().get("values", [[]])[0]
    return row_idx, row_data

async def update_google_sheet_row(sheet_id: str, row_index: int, data: dict, access_token: str, phone_number: str = None, tab_name: str = None):
    """Updates an existing row, aligning keys to headers and merging with existing data."""
    import httpx
    headers = await fetch_google_sheets_headers(sheet_id, access_token, tab_name=tab_name)
    if not headers: return

    # 1. Fetch existing row to avoid wiping data
    last_col = index_to_a1(len(headers) - 1)
    range_notation = f"'{tab_name}'!A{row_index}:{last_col}{row_index}" if tab_name else f"A{row_index}:{last_col}{row_index}"
    url_read = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{range_notation}"
    
    existing_row = []
    async with httpx.AsyncClient() as client:
        resp = await client.get(url_read, headers={"Authorization": f"Bearer {access_token}"})
        data_json = resp.json()
        if "values" in data_json and data_json["values"]:
            existing_row = data_json["values"][0]
    
    # 2. Align & Merge
    merged_row = []
    for i, h in enumerate(headers):
        key = h.lower()
        new_val = None
        for k, v in data.items():
            if k.lower() == key:
                new_val = v
                break
        
        current_val = existing_row[i] if i < len(existing_row) else ""
        if new_val is not None and str(new_val).strip() != "":
            merged_row.append(str(new_val))
        elif not str(current_val).strip() and phone_number and any(term in key for term in ["phone", "mobile"]):
            merged_row.append(phone_number)
        else:
            merged_row.append(str(current_val))

    # 3. Write back
    url_write = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{range_notation}?valueInputOption=USER_ENTERED"
    async with httpx.AsyncClient() as client:
        await client.put(url_write, json={"values": [merged_row]}, headers={"Authorization": f"Bearer {access_token}"})

async def write_to_google_sheets(sheet_id: str, data: dict, access_token: str, phone_number: str = None, tab_name: str = None):
    """Appends a new row to a sheet, aligning keys to headers."""
    import httpx
    headers = await fetch_google_sheets_headers(sheet_id, access_token, tab_name=tab_name)
    if not headers: return
    
    row_values = [""] * len(headers)
    for i, h in enumerate(headers):
        key = h.lower()
        found = False
        for k, v in data.items():
            if k.lower() == key:
                row_values[i] = str(v)
                found = True
                break
        if not found and any(term in key for term in ["phone", "mobile"]) and phone_number:
            row_values[i] = phone_number

    range_notation = f"'{tab_name}'!A1:CV1" if tab_name else "A1:CV1"
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{range_notation}:append?valueInputOption=USER_ENTERED"
    
    print(f"[SHEETS] Appending to {sheet_id} [{range_notation}]...")
    async with httpx.AsyncClient() as client:
        resp = await client.post(url, json={"values": [row_values]}, headers={"Authorization": f"Bearer {access_token}"})
        print(f"[SHEETS] Append Response: {resp.status_code} - {resp.text}")

async def clear_google_sheets_range(sheet_id: str, access_token: str, tab_name: str = None, range_a1: str = None):
    """Clears a range or entire tab."""
    import httpx
    range_notation = f"'{tab_name}'!{range_a1 or 'A1:Z100'}" if tab_name else (range_a1 or 'A1:Z100')
    url = f"https://www.googleapis.com/sheets/v4/spreadsheets/{sheet_id}/values/{range_notation}:clear"
    async with httpx.AsyncClient() as client:
        await client.post(url, headers={"Authorization": f"Bearer {access_token}"})


async def read_calendar_events(start_time: str, end_time: str, access_token: str) -> list:
    """Read existing calendar events in a time range to check availability"""
    import httpx
    from datetime import datetime
    
    try:
        # Format times for Google Calendar API (ISO 8601)
        url = f"https://www.googleapis.com/calendar/v3/calendars/primary/events"
        params = {
            "timeMin": start_time,
            "timeMax": end_time,
            "singleEvents": "true",
            "orderBy": "startTime"
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                params=params,
                headers={"Authorization": f"Bearer {access_token}"}
            )
            
            if response.status_code == 200:
                data = response.json()
                events = data.get("items", [])
                print(f"[AUTO] Found {len(events)} existing events in requested time range")
                return events
            else:
                print(f"[AUTO] Failed to read calendar: {response.status_code} - {response.text}")
                return []
    except Exception as e:
        print(f"[AUTO] Error reading calendar: {e}")
        return []

def check_availability(requested_start: str, requested_end: str, existing_events: list) -> dict:
    """Check if a time slot is available by detecting conflicts with existing events"""
    from datetime import datetime
    
    try:
        # Parse requested times
        req_start = datetime.fromisoformat(requested_start.replace('Z', '+00:00'))
        req_end = datetime.fromisoformat(requested_end.replace('Z', '+00:00'))
        
        conflicts = []
        for event in existing_events:
            # Get event times
            event_start_str = event.get('start', {}).get('dateTime', event.get('start', {}).get('date'))
            event_end_str = event.get('end', {}).get('dateTime', event.get('end', {}).get('date'))
            
            if not event_start_str or not event_end_str:
                continue
            
            event_start = datetime.fromisoformat(event_start_str.replace('Z', '+00:00'))
            event_end = datetime.fromisoformat(event_end_str.replace('Z', '+00:00'))
            
            # Check for overlap: conflicts exist if intervals intersect
            if req_start < event_end and req_end > event_start:
                conflicts.append({
                    "event_id": event.get('id'),  # Include ID for potential updates
                    "title": event.get('summary', 'Busy'),
                    "start": event_start_str,
                    "end": event_end_str
                })
        
        if conflicts:
            return {
                "available": False,
                "conflicts": conflicts,
                "message": f"Time slot unavailable - {len(conflicts)} conflict(s) found"
            }
        else:
            return {
                "available": True,
                "conflicts": [],
                "message": "Time slot available"
            }
    except Exception as e:
        print(f"[AUTO] Error checking availability: {e}")
        return {"available": True, "conflicts": [], "message": "Unable to verify - proceeding"}

async def create_google_calendar_event(event: dict, access_token: str) -> dict:
    """Create event in Google Calendar"""
    import httpx
    
    print(f"[CALENDAR API] Creating event: {event.get('summary', 'N/A')}")
    print(f"[CALENDAR API] Access token: {access_token[:20] if access_token else 'None'}...")
    
    try:
        url = "https://www.googleapis.com/calendar/v3/calendars/primary/events"
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                json=event,
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json"
                }
            )
            
            print(f"[CALENDAR API] Response status: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                print(f"[CALENDAR API] SUCCESS! Event ID: {result.get('id')}")
                print(f"[CALENDAR API] Event Link: {result.get('htmlLink')}")
                return {"success": True, "event_id": result.get("id"), "link": result.get("htmlLink")}
            else:
                print(f"[CALENDAR API] FAILED: {response.text}")
                return {"success": False, "error": response.text}
                
    except Exception as e:
        print(f"[CALENDAR API] Exception: {e}")
        return {"success": False, "error": str(e)}

async def update_google_calendar_event(event_id: str, event_updates: dict, access_token: str) -> dict:
    """Update an existing event in Google Calendar"""
    import httpx
    
    print(f"[CALENDAR API] Updating event: {event_id}")
    print(f"[CALENDAR API] New details: {event_updates.get('summary', 'N/A')}")
    
    try:
        url = f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{event_id}"
        
        async with httpx.AsyncClient() as client:
            response = await client.patch(
                url,
                json=event_updates,
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json"
                }
            )
            
            print(f"[CALENDAR API] Update Response status: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                print(f"[CALENDAR API] UPDATE SUCCESS! Event ID: {result.get('id')}")
                print(f"[CALENDAR API] Event Link: {result.get('htmlLink')}")
                return {"success": True, "event_id": result.get("id"), "link": result.get("htmlLink"), "updated": True}
            else:
                print(f"[CALENDAR API] UPDATE FAILED: {response.text}")
                return {"success": False, "error": response.text}
                
    except Exception as e:
        print(f"[CALENDAR API] Update Exception: {e}")
        return {"success": False, "error": str(e)}

async def delete_google_calendar_event(event_id: str, access_token: str) -> dict:
    """Delete an event from Google Calendar"""
    import httpx
    
    print(f"[CALENDAR API] Deleting event: {event_id}")
    
    try:
        url = f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{event_id}"
        
        async with httpx.AsyncClient() as client:
            response = await client.delete(
                url,
                headers={
                    "Authorization": f"Bearer {access_token}"
                }
            )
            
            print(f"[CALENDAR API] Delete Response status: {response.status_code}")
            
            # 204 No Content = successful deletion
            if response.status_code in [200, 204]:
                print(f"[CALENDAR API] DELETE SUCCESS! Event {event_id} removed")
                return {"success": True, "event_id": event_id, "deleted": True}
            else:
                print(f"[CALENDAR API] DELETE FAILED: {response.text}")
                return {"success": False, "error": response.text}
                
    except Exception as e:
        print(f"[CALENDAR API] Delete Exception: {e}")
        return {"success": False, "error": str(e)}

async def get_upcoming_bookings(access_token: str, max_results: int = 10) -> list:
    """Get upcoming test drive bookings from the calendar"""
    import httpx
    from datetime import datetime, timezone
    
    print(f"[CALENDAR API] Fetching upcoming bookings...")
    
    try:
        url = "https://www.googleapis.com/calendar/v3/calendars/primary/events"
        now = datetime.now(timezone.utc).isoformat()
        
        params = {
            "timeMin": now,
            "maxResults": max_results,
            "singleEvents": "true",
            "orderBy": "startTime",
            "q": "Test Drive"  # Search for test drive events
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                params=params,
                headers={
                    "Authorization": f"Bearer {access_token}"
                }
            )
            
            if response.status_code == 200:
                events = response.json().get("items", [])
                print(f"[CALENDAR API] Found {len(events)} upcoming test drive bookings")
                
                bookings = []
                for event in events:
                    bookings.append({
                        "event_id": event.get("id"),
                        "summary": event.get("summary"),
                        "start": event.get("start", {}).get("dateTime"),
                        "end": event.get("end", {}).get("dateTime"),
                        "description": event.get("description", ""),
                        "link": event.get("htmlLink")
                    })
                return bookings
            else:
                print(f"[CALENDAR API] Failed to fetch bookings: {response.text}")
                return []
                
    except Exception as e:
        print(f"[CALENDAR API] Get bookings exception: {e}")
        return []

async def send_automation_email(email_content: dict, rule: dict, automation_data: dict) -> dict:
    """Send automation email (placeholder implementation)"""
    
    # This would integrate with your email service (SendGrid, SES, etc.)
    print(f"[AUTO] Email to be sent: {email_content.get('subject', 'No subject')}")
    
    return {
        "success": True,
        "message": "Email queued for delivery",
        "subject": email_content.get("subject", "")
    }
