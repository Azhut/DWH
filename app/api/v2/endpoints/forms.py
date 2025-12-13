from fastapi import APIRouter, HTTPException, Depends
from app.api.v2.schemas.forms import FormCreateRequest, FormResponse, FormsListResponse
from app.core.dependencies import get_forms_service
from app.data.services.forms_service import FormsService

router = APIRouter()


@router.get("/forms", response_model=FormsListResponse)
async def list_forms(svc: FormsService = Depends(get_forms_service)):
    docs = await svc.list_forms()
    forms = []
    for d in docs:
        forms.append(FormResponse(
            id=d.get("id"),
            name=d.get("name"),
            spravochno_keywords=d.get("spravochno_keywords", []),
            skip_sheets=d.get("skip_sheets", []),
            created_at=d.get("created_at")
        ))
    return FormsListResponse(forms=forms)


@router.get("/forms/{form_id}", response_model=FormResponse)
async def get_form(form_id: str, svc: FormsService = Depends(get_forms_service)):
    doc = await svc.get_form(form_id)
    if not doc:
        raise HTTPException(404, "Form not found")
    return FormResponse(
        id=doc.get("id"),
        name=doc.get("name"),
        spravochno_keywords=doc.get("spravochno_keywords", []),
        skip_sheets=doc.get("skip_sheets", []),
        created_at=doc.get("created_at")
    )


@router.post("/forms")
async def create_form(payload: FormCreateRequest, svc: FormsService = Depends(get_forms_service)):
    doc = await svc.create_form(payload.model_dump())
    return {"message": "Form created", "form": doc}


@router.put("/forms/{form_id}")
async def update_form(form_id: str, payload: FormCreateRequest, svc: FormsService = Depends(get_forms_service)):
    updated = await svc.update_form(form_id, payload.model_dump())
    if not updated:
        raise HTTPException(404, "Form not found")
    return {"message": "Form updated", "form": updated}


@router.delete("/forms/{form_id}")
async def delete_form(form_id: str, svc: FormsService = Depends(get_forms_service)):
    ok = await svc.delete_form(form_id)
    if not ok:
        raise HTTPException(404, "Form not found")
    return {"message": "Form deleted", "id": form_id}
