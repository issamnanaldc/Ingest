from pydantic import BaseModel
from pathlib import Path

class IngestConfig(BaseModel):
    connection_string: str
    table_name: str

    class Config:
        frozen = True  # To make the model immutable like the frozen dataclass


class ProcessConfig(BaseModel):
    connection_string: str
    table_name: str
    container_name: str

    class Config:
        frozen = True  # To make the model immutable like the frozen dataclass
