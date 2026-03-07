from typing import List

from fastapi import UploadFile

from app.core.exceptions import FormValidationError, RequestValidationError
from app.domain.form import validate_form_id


class RequestValidator:
    """Request-level validation for upload endpoint."""

    @staticmethod
    def validate_request(files: List[UploadFile], form_id: str) -> str:
        try:
            form_id = validate_form_id(form_id)
        except FormValidationError as exc:
            raise RequestValidationError(
                message=exc.message,
                http_status=400,
                domain="form.validation",
                meta={"form_id": exc.form_id},
            ) from exc

        if not files:
            raise RequestValidationError(
                message="No files were provided for upload",
                http_status=400,
                domain="upload.request",
                meta={"form_id": form_id},
            )

        for index, file in enumerate(files):
            filename = (getattr(file, "filename", None) or "").strip()
            if not filename:
                raise RequestValidationError(
                    message=f"File at index {index} has empty filename",
                    http_status=400,
                    domain="upload.request",
                    meta={"form_id": form_id, "file_index": index},
                )

        return form_id.strip()
