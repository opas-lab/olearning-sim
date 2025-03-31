from typing import Optional, List
import uuid
import datetime

from pydantic import BaseModel


class TaskModel(BaseModel):
    train_path: str='tasktest.training_models'
    release_path: str='tasktest.released_models'


class TaskData(BaseModel):
    train_data: str='data_test_train'
    test_data: str='data_test_test'


class TaskStaticSelection(BaseModel):
    models: List[str]=['FindX']
    regions: List[str]=['Singapore']


class TaskDynamicSelection(BaseModel):
    p: float=0.5
    alpha: float=0.5
    cooldown: int=7200


class TaskTrain(BaseModel):
    batch_size: int=64
    learning_rate: float=0.01
    max_round: int=20
    max_local_epoch: int=5
    metrics: List[str]=['acc', 'auc', 'loss']
    threshold_n: int=10
    convergence_threshold: Optional[float]=0.5


class TaskAggregation(BaseModel):
    method: str='FedAvg'
    sample_size: int=4096
    timeout: int=3600*6
    minimum_sample_size: int=1024


class TaskEvokeCondition(BaseModel):
    time: List[str]=[datetime.time(3)]
    bias: int=7200
    alpha: float=1.0


class TaskAcc(BaseModel):
    train_acc: float=0.
    train_auc: float=0.
    train_loss: float=1000.

    test_acc: float=0.
    test_auc: float=0.
    test_loss: float=1000.


class TaskStatus(BaseModel):
    create_time: datetime.datetime=None
    round_start_time: datetime.datetime=None
    current_round: int=0
    status: str="TRAINING"
    round_status: str="TRAINING"
    current_acc: TaskAcc=TaskAcc()


class Task(BaseModel):
    id: str=uuid.uuid1().hex
    model: Optional[TaskModel]=TaskModel()
    data: Optional[TaskData]=TaskData()
    static_selection: Optional[TaskStaticSelection]=TaskStaticSelection()
    dynamic_selection: Optional[TaskDynamicSelection]=TaskDynamicSelection()
    train: Optional[TaskTrain]=TaskTrain()
    aggregation: Optional[TaskAggregation]=TaskAggregation()
    evoke_condition: Optional[TaskEvokeCondition]=TaskEvokeCondition()
    status: Optional[TaskStatus]=TaskStatus()


if __name__ == '__main__':
    import json
    task = Task()
    print(task.json(indent=2))
    task1 = Task.parse_raw(task.json())
    assert task1.json() == task.json()
