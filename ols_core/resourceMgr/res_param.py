from pydantic import BaseModel
from typing import Optional
import datetime

class ResourceDetail(BaseModel):
    task_id      : Optional[str] = "0"
    cpu          : Optional[float] = 0.0
    mem          : Optional[float] = 0.0
    request_time : Optional[datetime.datetime] = datetime.datetime.now()
    release_time : Optional[datetime.datetime] = datetime.datetime.now()

class Resource(BaseModel):
    cpu          : Optional[float] = 0.0
    mem          : Optional[float] = 0.0
