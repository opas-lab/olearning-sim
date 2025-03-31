from typing import Optional, List
import uuid
import hashlib
from pydantic import BaseModel

from ols.ofl_commons.infrastructure.model import Model

class MetricFragments(BaseModel):
    train_acc_fragment: Optional[float]=None
    train_loss_fragment: Optional[float]=None
    train_tp_fragment: Optional[List[int]]=None
    train_tn_fragment: Optional[List[int]]=None
    train_fp_fragment: Optional[List[int]]=None
    train_fn_fragment: Optional[List[int]]=None

    test_acc_fragment: Optional[float]=None
    test_loss_fragment: Optional[float]=None
    test_tp_fragment: Optional[List[int]]=None
    test_tn_fragment: Optional[List[int]]=None
    test_fp_fragment: Optional[List[int]]=None
    test_fn_fragment: Optional[List[int]]=None

    thresholds: Optional[int]=None

class MessageHeader(BaseModel):
    clientId: str=hashlib.sha256().hexdigest()
    messageId: uuid.UUID=uuid.uuid4()
    messageType: str=''

class MessageData(BaseModel):
    task_id: str=None
    simulation: Optional[bool]=False
    training_round: int=0
    local_train_sample_size: int=0
    local_test_sample_size: int=0
    model: Optional[Model]=Model()
    metrics: Optional[MetricFragments]=MetricFragments()

class Fragment(BaseModel):
    header: MessageHeader=MessageHeader()
    data: MessageData=MessageData()


if __name__ == '__main__':
    clientId = hashlib.sha256().hexdigest()
    messageId = uuid.uuid4()
    print(f"client id is: {clientId}, message id is: {messageId}")
    frag = Fragment(
        header = MessageHeader(
            clientId = clientId,
            messageId = messageId,
            messageType = "PUBLISH_MODEL_MESSAGE",
        ),
        data = MessageData(
            task_id=uuid.uuid1().hex,
            training_round=2,
            local_train_sample_size=1022,
            local_test_sample_size=128,
            metrics=MetricFragments(
                train_acc_fragment=0.9,
                train_loss_fragment=0.032,
                train_thresholds=3,
                train_tp_fragment=[5, 6, 7],
            )
        )
    )
    print(frag.json(indent=2))
