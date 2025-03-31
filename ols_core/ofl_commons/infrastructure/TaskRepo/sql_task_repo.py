import logging
import sqlalchemy
import pandas as pd
import datetime
from pathlib import Path

from ols.ofl_commons.infrastructure.TaskRepo.task_domain import Task, TaskStatus,\
    TaskModel, TaskData, TaskAggregation, TaskTrain
from ols.ofl_commons.infrastructure.TaskRepo.task_repo import TaskRepo

class SqlTaskRepo(TaskRepo):
    def __init__(self, host: str, user: str, password: str, port: int, database: str):
        self._host = host
        self._user = user
        self._password = password
        self._port = port
        self._database = database

    @property
    def engine(self):
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self._user}:{self._password}@{self._host}:{self._port}/{self._database}"
        )
        return engine

    # Get task info with task id from SQL database
    def get_task(self, task_id: str) -> Task:
        sql = f'''
        SELECT * FROM tasks JOIN train_parameters 
        ON tasks.task_id = train_parameters.task_id
        '''

        try:
            task_info_df = pd.read_sql(sql, self.engine)
            task_info = task_info_df.loc[:,~task_info_df.columns.duplicated()].set_index('task_id', inplace=False)
            task_info_dict = task_info.loc[task_id].to_dict()
            task = Task(
                id = task_id,
                model = TaskModel(
                    train_path = task_info_dict['model_cache_path'],
                    release_path = task_info_dict['model_cache_path']
                ),
                data = TaskData(
                    train_data = task_info_dict['train_data_path'],
                    test_data = task_info_dict['test_data_path'],
                ),
                train = TaskTrain(
                    batch_size = task_info_dict['batch_size'],
                    learning_rate = task_info_dict['learning_rate'],
                    max_round = task_info_dict['max_round'],
                    max_local_epoch = task_info_dict['max_local_epoch'],
                    threshold_n = task_info_dict['cm_n_thresholds'],
                    convergence_threshold = task_info_dict['convergence_threshold']
                ),
                aggregation = TaskAggregation(
                    method = task_info_dict['aggregate_method'],
                    sample_size = task_info_dict['aggregate_threshold_high'],
                    minimum_sample_size = task_info_dict['aggregate_threshold_low'],
                    timeout = task_info_dict['timeout']
                ),
                status = TaskStatus(
                    create_time = task_info_dict['create_time'],
                    round_start_time = task_info_dict['round_start_time'],
                    current_round = task_info_dict['current_round'],
                    round_status = task_info_dict['round_status'],
                    status = task_info_dict['status']
                )
            )
        except Exception as e:
            logging.error(f"[Get task]: Get task error.")
            logging.exception(e)
            return None
        return task


    def set_status(self, task_id: str, task: Task):
        logging.info(f"[Set status]: Task {task_id} status is {task.status}")

        train_metrics_sql = sqlalchemy.text(f"""
            INSERT INTO train_metrics (task_id, train_round, train_loss, train_acc, train_auc, test_loss, test_acc, test_auc)
            VALUES (:task_id, {task.status.current_round},
                    {task.status.current_acc.train_loss}, {task.status.current_acc.train_acc}, {task.status.current_acc.train_auc},
                    {task.status.current_acc.test_loss}, {task.status.current_acc.test_acc}, {task.status.current_acc.test_auc})""")

        tasks_sql = sqlalchemy.text(f"""UPDATE tasks SET status = :status, current_round = :current_round,
            round_status = :round_status, round_start_time = :round_start_time WHERE task_id = :task_id""")

        train_parameters_sql = sqlalchemy.text(f"""UPDATE train_parameters 
            SET model_cache_path = :model_cache_path WHERE task_id = :task_id""")

        with self.engine.connect() as conn:
            trans = conn.begin()

            try:
                train_metrics_result = conn.execute(train_metrics_sql, task_id = task_id)
                if train_metrics_result.rowcount != 1:
                    raise Exception("[Set status]: Failed to insert train metrics.")

                tasks_result = conn.execute(tasks_sql, status = task.status.status, current_round = task.status.current_round, 
                                    round_status = task.status.round_status, round_start_time = task.status.round_start_time, task_id = task_id)
                if tasks_result.rowcount != 1:
                    raise Exception("[Set status]: Failed to update tasks.")

                model_cache_path = Path(task.model.train_path)
                if model_cache_path.parent == Path(''):
                    train_path = f"{task_id}_{task.status.current_round}_result_model.mnn"
                else:
                    train_path = f"{str(model_cache_path.parent)}/{task_id}_{task.status.current_round}_result_model.mnn"
                train_parameters_result = conn.execute(train_parameters_sql, task_id = task_id, model_cache_path = train_path)
                
                if train_parameters_result.rowcount != 1:
                    raise Exception("[Set status]: Failed to update train parameters.")

                trans.commit()
                logging.info(f"Transaction committed successfully.")
            except Exception as e:
                trans.rollback()
                logging.error(f"Transaction rolled back: {e}")

