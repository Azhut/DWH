import uuid
from datetime import datetime
from fastapi import APIRouter, HTTPException

from app.core.database import mongo_connection
from app.api.v2.schemas.forms import (
    FormCreate,
    FormUpdate,
    FormResponse,
    FormsListResponse,
    CreateFormResponse,
    UpdateFormResponse,
    DeleteFormResponse,
)

router = APIRouter()


@router.get("/forms", response_model=FormsListResponse)
async def get_forms():
    db = mongo_connection.get_database()
    forms = await db.Forms.find().to_list(None)
    return {"forms": forms}


@router.get("/forms/{form_id}", response_model=FormResponse)
async def get_form(form_id: str):
    db = mongo_connection.get_database()
    form = await db.Forms.find_one({"id": form_id})
    if not form:
        raise HTTPException(404, "Form not found")
    return form


@router.post("/forms", response_model=CreateFormResponse)
async def create_form(payload: FormCreate):
    db = mongo_connection.get_database()
    form = {
        "id": str(uuid.uuid4()),
        "name": payload.name,
        "requisites": payload.requisites.model_dump() if payload.requisites else {},
        "created_at": datetime.utcnow(),
    }
    await db.Forms.insert_one(form)
    return {"message": "Form created", "form": form}


@router.put("/forms/{form_id}", response_model=UpdateFormResponse)
async def update_form(form_id: str, payload: FormUpdate):
    db = mongo_connection.get_database()
    update = {}
    if payload.name is not None:
        update["name"] = payload.name
    if payload.requisites is not None:
        update["requisites"] = payload.requisites.model_dump()

    result = await db.Forms.update_one({"id": form_id}, {"$set": update})
    if result.matched_count == 0:
        raise HTTPException(404, "Form not found")

    form = await db.Forms.find_one({"id": form_id})
    return {"message": "Form updated", "form": form}


@router.delete("/forms/{form_id}", response_model=DeleteFormResponse)
async def delete_form(form_id: str):
    db = mongo_connection.get_database()
    result = await db.Forms.delete_one({"id": form_id})
    if result.deleted_count == 0:
        raise HTTPException(404, "Form not found")
    return {"message": "Form deleted", "id": form_id}
