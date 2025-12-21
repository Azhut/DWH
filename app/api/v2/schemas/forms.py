from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime


class Requisites(BaseModel):
    skip_sheets: List[int] = Field(default_factory=list)


class FormBase(BaseModel):
    name: str
    requisites: Optional[Requisites] = None


class FormCreate(FormBase):
    pass


class FormUpdate(BaseModel):
    name: Optional[str] = None
    requisites: Optional[Requisites] = None


class FormResponse(FormBase):
    id: str
    created_at: datetime


class FormsListResponse(BaseModel):
    forms: List[FormResponse]


class CreateFormResponse(BaseModel):
    message: str
    form: FormResponse


class UpdateFormResponse(BaseModel):
    message: str
    form: FormResponse


class DeleteFormResponse(BaseModel):
    message: str
    id: str
