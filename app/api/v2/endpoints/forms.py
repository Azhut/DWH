from fastapi import APIRouter, Depends, HTTPException

from app.api.v2.schemas.forms import (
    FormCreate,
    FormUpdate,
    FormResponse,
    FormsListResponse,
    CreateFormResponse,
    UpdateFormResponse,
    DeleteFormResponse,
)
from app.core.dependencies import get_form_service
from app.domain.form.service import FormService

router = APIRouter()


@router.get("/forms", response_model=FormsListResponse)
async def get_forms(form_service: FormService = Depends(get_form_service)):
    forms = await form_service.list_forms()
    return {"forms": forms}


@router.get("/forms/{form_id}", response_model=FormResponse)
async def get_form(form_id: str, form_service: FormService = Depends(get_form_service)):
    form = await form_service.get_form(form_id)
    if not form:
        raise HTTPException(404, "Form not found")
    return form


@router.post("/forms", response_model=CreateFormResponse)
async def create_form(payload: FormCreate, form_service: FormService = Depends(get_form_service)):
    payload_dict = payload.model_dump()
    if payload.requisites:
        payload_dict["requisites"] = payload.requisites.model_dump()
    form = await form_service.create_form(payload_dict)
    return {"message": "Form created", "form": form}


@router.put("/forms/{form_id}", response_model=UpdateFormResponse)
async def update_form(
    form_id: str,
    payload: FormUpdate,
    form_service: FormService = Depends(get_form_service),
):
    update = {}
    if payload.name is not None:
        update["name"] = payload.name
    if payload.requisites is not None:
        update["requisites"] = payload.requisites.model_dump()
    form = await form_service.update_form(form_id, update)
    if form is None:
        raise HTTPException(404, "Form not found")
    return {"message": "Form updated", "form": form}


@router.delete("/forms/{form_id}", response_model=DeleteFormResponse)
async def delete_form(form_id: str, form_service: FormService = Depends(get_form_service)):
    deleted = await form_service.delete_form(form_id)
    if not deleted:
        raise HTTPException(404, "Form not found")
    return {"message": "Form deleted", "id": form_id}
