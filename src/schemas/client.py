from pydantic import BaseModel, Field, field_validator
from src.models.client import ClientStatus
from datetime import datetime

class register_client_request(BaseModel):
    name: str = Field(min_length=1, max_length=150)

    @field_validator("name", mode="before")
    @classmethod
    def name_must_not_be_blank(cls, value: str) -> str:
        if not isinstance(value, str):
            return value
        value = value.strip()
        if not value:
            raise ValueError("name cannot be empty or whitespace-only")
        return value

class register_client_response(BaseModel):
    id:int
    name:str
    status:ClientStatus
    created_at:datetime
