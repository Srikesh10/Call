# CSV Knowledge Base Import API
# Add this to server.py to enable CSV imports

from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
import base64
import csv
import io
import json

from dependencies import get_current_user

router = APIRouter(prefix="/api/knowledge", tags=["knowledge"])

@router.get("/health")
async def health_check():
    """Simple health check without authentication"""
    return {
        "status": "healthy",
        "message": "Knowledge base API is running"
    }

@router.get("/test")
async def test_endpoint(user: dict = Depends(get_current_user)):
    """Simple test endpoint to verify API is working"""
    print(f"[DEBUG] Test endpoint called by user: {user}")
    return {
        "status": "API working",
        "user_id": user.get('id'),
        "message": "Knowledge base API is accessible"
    }

@router.post("/import-csv")
async def import_knowledge_base_csv(
    file: UploadFile = File(..., description="CSV file for knowledge base"),
    user: dict = Depends(get_current_user),
    category_mapping: str = "{}"
):
    """
    Import knowledge base from CSV file and store in app_options.knowledge_base column
    CSV format: title,content,category,tags,priority
    """
    
    print(f"[DEBUG] CSV IMPORT STARTED!")
    print(f"[DEBUG] User object: {user}")
    print(f"[DEBUG] User type: {type(user)}")
    print(f"[DEBUG] User ID: {user.id if user else 'NO_USER'}")
    print(f"[DEBUG] User email: {user.email if user else 'NO_EMAIL'}")
    print(f"[DEBUG] File: {file}")
    print(f"[DEBUG] Category mapping: {category_mapping}")
    
    # Check if user is None
    if not user:
        print(f"[ERROR] User is None! Authentication failed!")
        raise HTTPException(status_code=401, detail="Authentication failed: No user found")
    
    try:
        # Read and validate CSV file
        content = await file.read()
        content = content.decode('utf-8')
        
        # Parse CSV
        csv_reader = csv.reader(io.StringIO(content))
        rows_imported = 0
        knowledge_items = [] # Initialize list
        errors = []
        
        print(f"[DEBUG] CSV IMPORT STARTED!")
        print(f"[DEBUG] User: {user}")
        print(f"[DEBUG] File: {file}")
        
        # Skip header row and store raw data
        for row_num, row in enumerate(csv_reader, 1):
            try:
                print(f"[DEBUG] Processing Row {row_num}: {row}")
                
                # Skip header row
                if row_num == 1 and any(header.lower() in [col.lower() for col in row if col] for header in ['title', 'content', 'model', 'brand', 'year', 'price']):
                    print(f"[DEBUG] Skipping header row: {row}")
                    continue
                
                # Store raw CSV data as-is in knowledge_base column
                if row and any(cell.strip() for cell in row if cell):  # Skip empty rows
                    # Create knowledge item with raw CSV data
                    item = {
                        "title": row[0].strip() if row[0] else 'Unknown',
                        "content": f"CSV Data: {', '.join(row)}",  # Store entire row as content
                        "category": "inventory",
                        "tags": ["csv", "raw_data"],
                        "priority": 1,
                        "is_active": True,
                        "raw_csv": row  # Store raw CSV data for LLM processing
                    }
                    
                    print(f"[DEBUG] Created raw item: {item}")
                    knowledge_items.append(item)
                    rows_imported += 1
                
            except Exception as e:
                print(f"[ERROR] Row processing failed: {e}")
                errors.append(f"Row {row_num}: {str(e)}")
        
        print(f"[DEBUG] Total items created: {len(knowledge_items)}")
        print(f"[DEBUG] Total rows imported: {rows_imported}")
        print(f"[DEBUG] Total errors: {len(errors)}")
        
        # Store in app_options.knowledge_base column
        if knowledge_items:
            from backend.supabase_client import supabase_adapter
            
            print(f"[DEBUG] Attempting to save {len(knowledge_items)} knowledge items")
            
            # Get current app_options
            current_options = supabase_adapter.get_app_options(user.id)
            print(f"[DEBUG] Current app_options keys: {list(current_options.keys())}")
            existing_knowledge = current_options.get('knowledge_base', [])
            print(f"[DEBUG] Existing knowledge items: {len(existing_knowledge)}")
            
            # Merge with existing knowledge base
            merged_knowledge = existing_knowledge + knowledge_items
            print(f"[DEBUG] Merged knowledge items: {len(merged_knowledge)}")
            
            # Update app_options with new knowledge base - SIMPLE APPROACH
            update_data = {
                'knowledge_base': merged_knowledge,
                'updated_at': 'now()'
            }
            print(f"[DEBUG] Update data being sent: {list(update_data.keys())}")
            
            success = supabase_adapter.save_app_options(user.id, update_data)
            print(f"[DEBUG] Save result: {success}")
            
            if success:
                return {
                    "status": "completed",
                    "rows_imported": rows_imported,
                    "errors": errors,
                    "message": f"Successfully imported {rows_imported} knowledge items to app_options"
                }
            else:
                return {
                    "status": "failed",
                    "rows_imported": 0,
                    "errors": ["Failed to save to app_options"],
                    "message": "Database error occurred"
                }
        else:
            print(f"[DEBUG] No knowledge items to save!")
            return {
                "status": "failed",
                "rows_imported": 0,
                "errors": errors,
                "message": "No valid data found in CSV"
            }
            
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Import failed: {str(e)}")

@router.get("/categories")
async def get_knowledge_categories(user: dict = Depends(get_current_user)):
    """Get available knowledge base categories"""
    return {
        "categories": [
            {"value": "product", "label": "Products & Services"},
            {"value": "policy", "label": "Policies & Procedures"},
            {"value": "process", "label": "Processes & Workflows"},
            {"value": "faq", "label": "FAQ & Help"},
            {"value": "general", "label": "General Information"}
        ]
    }

@router.get("/items")
async def get_knowledge_items(
    user: dict = Depends(get_current_user),
    category: str = None,
    limit: int = 50
):
    """Get knowledge base items from app_options.knowledge_base column"""
    try:
        from backend.supabase_client import supabase_adapter
        
        # Get app_options which contains knowledge_base
        options = supabase_adapter.get_app_options(user.id)
        knowledge_base = options.get('knowledge_base', [])
        
        # Filter by category if specified
        if category:
            knowledge_base = [item for item in knowledge_base if item.get('category') == category]
        
        # Apply limit
        if limit:
            knowledge_base = knowledge_base[:limit]
        
        return {
            "status": "success",
            "items": knowledge_base,
            "total": len(knowledge_base)
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }

@router.post("/items")
async def add_knowledge_item(
    item: dict,
    user: dict = Depends(get_current_user)
):
    """Add single knowledge item to app_options.knowledge_base"""
    try:
        from backend.supabase_client import supabase_adapter
        
        # Get current app_options
        current_options = supabase_adapter.get_app_options(user.id)
        existing_knowledge = current_options.get('knowledge_base', [])
        
        # Add new item
        new_item = {
            "title": item.get('title'),
            "content": item.get('content'),
            "category": item.get('category', 'general'),
            "tags": item.get('tags', []),
            "priority": item.get('priority', 1),
            "is_active": True
        }
        
        updated_knowledge = existing_knowledge + [new_item]
        
        # Update app_options
        success = supabase_adapter.save_app_options(user.id, {
            **current_options,
            'knowledge_base': updated_knowledge,
            'knowledge_base_enabled': True,
            'updated_at': 'now()'
        })
        
        if success:
            return {"status": "success", "message": "Knowledge item added"}
        else:
            return {"status": "error", "message": "Failed to add item"}
            
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.put("/items/{item_id}")
async def update_knowledge_item(
    item_id: str,
    item: dict,
    user: dict = Depends(get_current_user)
):
    """Update knowledge item in app_options.knowledge_base array"""
    try:
        from backend.supabase_client import supabase_adapter
        
        # Get current app_options
        current_options = supabase_adapter.get_app_options(user.id)
        knowledge_base = current_options.get('knowledge_base', [])
        
        # Find and update the item
        for i, kb_item in enumerate(knowledge_base):
            if kb_item.get('title') == item_id:  # Using title as ID for simplicity
                knowledge_base[i] = {**kb_item, **item}
                break
        
        # Update app_options
        success = supabase_adapter.save_app_options(user.id, {
            **current_options,
            'knowledge_base': knowledge_base,
            'updated_at': 'now()'
        })
        
        if success:
            return {"status": "success", "message": "Knowledge item updated"}
        else:
            return {"status": "error", "message": "Failed to update item"}
            
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.delete("/items/{item_id}")
async def delete_knowledge_item(
    item_id: str,
    user: dict = Depends(get_current_user)
):
    """Delete knowledge item from app_options.knowledge_base array"""
    try:
        from backend.supabase_client import supabase_adapter
        
        # Get current app_options
        current_options = supabase_adapter.get_app_options(user.id)
        knowledge_base = current_options.get('knowledge_base', [])
        
        # Remove the item
        knowledge_base = [item for item in knowledge_base if item.get('title') != item_id]
        
        # Update app_options
        success = supabase_adapter.save_app_options(user.id, {
            **current_options,
            'knowledge_base': knowledge_base,
            'updated_at': 'now()'
        })
        
        if success:
            return {"status": "success", "message": "Knowledge item deleted"}
        else:
            return {"status": "error", "message": "Failed to delete item"}
            
    except Exception as e:
        return {"status": "error", "message": str(e)}
