from typing import Optional

from pydantic import BaseModel


class Model(BaseModel):
    framework: str='mnn'
    payload: Optional[str]=None


if __name__ == '__main__':
    model = Model()
    print(model.json(indent=2))