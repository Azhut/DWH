from pydantic import BaseModel

class FileMetadataResponse(BaseModel):
    city: str
    year: int
    filename: str
    extension: str
    status: str