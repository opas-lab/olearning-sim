import os
import re
import json
import yaml
import pytz
import datetime
import sqlalchemy
import pandas as pd
from ols.simu_log import Logger
from ols.proto import taskService_pb2
from dateutil.parser import parse as date_parse
from sqlalchemy import MetaData, Table, update, select, and_, insert

from sqlalchemy.exc import OperationalError
from pymysql.err import OperationalError as PyMySQLOperationalError

import grpc
from ols.proto import deviceflow_pb2, deviceflow_pb2_grpc

# filepath
FILEPATH = os.path.abspath(os.path.dirname(__file__))   # utils.py文件所在的绝对路径
OLSPATH = os.path.abspath(os.path.join(FILEPATH, '../..')) # ols所在文件夹的绝对路径
DEVICEFLOW_CONFIG = f"{OLSPATH}/config/deviceflow_config.yaml" # deviceflow_config.yaml所在路径

# log
logger = Logger()
logger.set_logger(log_yaml = OLSPATH + "/config/repo_log.yaml")

class TaskTableRepo:
    def __init__(self, yaml_path:str):
        with open(yaml_path, 'r') as file:
            task_table_config = yaml.load(file, Loader=yaml.FullLoader)
        self._user = task_table_config["user"]
        self._password = task_table_config["password"]
        self._host = task_table_config["host"]
        self._port = str(task_table_config["port"])
        self._database = task_table_config["database"]
        self._table_name = task_table_config["table"]
        self._initialize_db()

    def _initialize_db(self):
        self._engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self._user}:{self._password}@{self._host}:{self._port}/{self._database}"
        )
        self._metadata = MetaData()
        self._table = Table(self._table_name, self._metadata, autoload=True, autoload_with=self._engine)

    def get_data_by_item_values(self, item: str, values: list):
        try:
            select_query = select([self._table]).where(self._table.c[item].in_(values))
            result = self._engine.execute(select_query).fetchall()
            if result:
                data = pd.DataFrame(result, columns=result[0].keys())
                return data
            else:
                return pd.DataFrame()
        except (OperationalError, PyMySQLOperationalError, ConnectionResetError) as e:
            # Retry重连
            logger.warning(task_id="", system_name="TaskMgr", module_name="TaskTableRepo",
                           message=f"OperationalError encountered, reinitializing database connection")
            self._initialize_db()
            try:
                select_query = select([self._table]).where(self._table.c[item].in_(values))
                result = self._engine.execute(select_query).fetchall()
                if result:
                    data = pd.DataFrame(result, columns=result[0].keys())
                    return data
                else:
                    return pd.DataFrame()
            except Exception as e:
                logger.error(task_id="", system_name="TaskMgr", module_name="TaskTableRepo",
                             message=f"[get_data_by_item_value]: Get item={item} and values={values} failed, because of {e}")
                return pd.DataFrame()
        except Exception as e:
            logger.error(task_id="", system_name="TaskMgr", module_name="TaskTableRepo",
                         message=f"[get_data_by_item_value]: Get item={item} and values={values} failed, because of {e}")
            return pd.DataFrame()

    #todo 可以用装饰器统一处理
    def set_item_value(self, task_id:str, item:str, value):
        try:
            update_query = update(self._table).where(self._table.c.task_id==task_id).values({item: value})
            self._engine.execute(update_query)
            return True
        except (OperationalError, PyMySQLOperationalError, ConnectionResetError) as e:
            # Retry重连
            logger.warning(task_id=task_id, system_name="TaskMgr", module_name="TaskTableRepo",
                           message=f"OperationalError encountered, reinitializing database connection for task_id={task_id}")
            self._initialize_db()
            try:
                update_query = update(self._table).where(self._table.c.task_id == task_id).values({item: value})
                self._engine.execute(update_query)
                return True
            except Exception as e:
                logger.error(task_id=task_id, system_name="TaskMgr", module_name="TaskTableRepo",
                             message=f"[set_task_status]: Set item={item} of task_id={task_id} failed, because of {e}")
                return False
        except Exception as e:
            logger.error(task_id=task_id, system_name="TaskMgr", module_name="TaskTableRepo",
                         message=f"[set_task_status]: Set item={item} of task_id={task_id} failed, because of {e}")
            return False

    def get_item_value(self, task_id: str, item: str):
        try:
            select_query = select([self._table.c[item]]).where(self._table.c.task_id == task_id)
            result = self._engine.execute(select_query).fetchone()
            if result:
                return result[0]
            else:
                return None
        except (OperationalError, PyMySQLOperationalError, ConnectionResetError) as e:
            # Retry重连
            logger.warning(task_id=task_id, system_name="TaskMgr", module_name="TaskTableRepo",
                           message=f"OperationalError encountered, reinitializing database connection for task_id={task_id}")
            self._initialize_db()
            try:
                select_query = select([self._table.c[item]]).where(self._table.c.task_id == task_id)
                result = self._engine.execute(select_query).fetchone()
                if result:
                    return result[0]
                else:
                    return None
            except Exception as e:
                logger.error(task_id=task_id, system_name="TaskMgr", module_name="TaskTableRepo",
                             message=f"[get_item_value]: Get item={item} of task_id={task_id} failed, because of {e}")
                return None
        except Exception as e:
            logger.error(task_id=task_id, system_name="TaskMgr", module_name="TaskTableRepo",
                        message=f"[get_item_value]: Get item={item} of task_id={task_id} failed, because of {e}")
            return None

    def check_task_in_database(self, task_id: str):
        try:
            select_query = select([self._table]).where(self._table.c.task_id == task_id)
            result = self._engine.execute(select_query).fetchone()
            if result:
                return True
            else:
                return False
        except (OperationalError, PyMySQLOperationalError, ConnectionResetError) as e:
            # Retry重连
            logger.warning(task_id=task_id, system_name="TaskMgr", module_name="TaskTableRepo",
                           message=f"OperationalError encountered, reinitializing database connection for task_id={task_id}")
            self._initialize_db()
            try:
                select_query = select([self._table]).where(self._table.c.task_id == task_id)
                result = self._engine.execute(select_query).fetchone()
                if result:
                    return True
                else:
                    return False
            except Exception as e:
                return False
        except Exception as e:
            return False

    def get_unique_item(self, item:str):
        '''
        example:
            task_distinct = task_repo.get_unique_item(item="task_id")
        '''
        try:
            select_query = select([eval(f"self._table.c.{item}.distinct()")])
            result = self._engine.execute(select_query).fetchall()
            if result:
                items = [row[0] for row in result]
                return items
            else:
                return []
        except (OperationalError, PyMySQLOperationalError, ConnectionResetError) as e:
            # Retry重连
            self._initialize_db()
            try:
                select_query = select([eval(f"self._table.c.{item}.distinct()")])
                result = self._engine.execute(select_query).fetchall()
                if result:
                    items = [row[0] for row in result]
                    return items
                else:
                    return []
            except Exception as e:
                return []
        except Exception as e:
            return []

    def get_values_by_conditions(self, item, **conditions):
        '''
        example:
            conditions = {'task_status': "UNDONE", "task_params": "hello_world"}
            task_ids = task_repo.get_values_by_conditions(item="task_id", **conditions)
        '''
        try:
            select_query = select([eval(f"self._table.c.{item}")]).where(
                and_((getattr(self._table.c, key) == value) for key, value in conditions.items()))
            result = self._engine.execute(select_query).fetchall()
            values = [row[0] for row in result]
            return values
        except (OperationalError, PyMySQLOperationalError, ConnectionResetError) as e:
            # Retry重连
            self._initialize_db()
            try:
                select_query = select([eval(f"self._table.c.{item}")]).where(
                    and_((getattr(self._table.c, key) == value) for key, value in conditions.items()))
                result = self._engine.execute(select_query).fetchall()
                values = [row[0] for row in result]
                return values
            except Exception as e:
                return []
        except Exception as e:
            return []

    def add_item(self, item: dict):
        """
        向数据库中添加新的记录。

        :param item_data: 一个字典，包含要添加的字段和值。
        :param log_params: 用于记录日志的参数字典。
        log_params = {"task_id": "aaa", "system_name": "Deviceflow", "module_name": "add_item"}
        """
        try:
            # 创建一个插入语句
            insert_stmt = insert(self._table).values(**item)
            # 执行插入操作
            self._engine.execute(insert_stmt)
            return True
        except (OperationalError, PyMySQLOperationalError, ConnectionResetError) as e:
            # Retry重连
            self._initialize_db()
            try:
                # 创建一个插入语句
                insert_stmt = insert(self._table).values(**item)
                # 执行插入操作
                self._engine.execute(insert_stmt)
                return True
            except Exception as e:
                return False
        except Exception as e:
            return False

    def set_item_values(self, identify_name: str, identify_value, values_to_update: dict):
        """
        在数据库里找字段identify_name的值为identify_value的数据条目，将其多个字段值更新为values_to_update，并赋予log日志字段log_params
        """
        try:
            update_query = update(self._table).where(eval(f"self._table.c.{identify_name}") == identify_value).values(
                values_to_update)
            self._engine.execute(update_query)
            return True
        except (OperationalError, PyMySQLOperationalError, ConnectionResetError) as e:
            # Retry重连
            logger.warning(task_id=identify_value, system_name="TaskMgr", module_name="TaskTableRepo",
                           message=f"OperationalError encountered, reinitializing database connection for task_id={identify_value}")
            self._initialize_db()
            try:
                update_query = update(self._table).where(
                    eval(f"self._table.c.{identify_name}") == identify_value).values(
                    values_to_update)
                self._engine.execute(update_query)
                return True
            except Exception as e:
                logger.error(task_id=identify_value, system_name="TaskMgr", module_name="TaskTableRepo",
                             message=f"[set_task_status]: Set values_to_update = {values_to_update} of identify_name={identify_name} failed, because of {e}")
                return False
        except Exception as e:
            logger.error(task_id=identify_value, system_name="TaskMgr", module_name="TaskTableRepo",
                         message=f"[set_task_status]: Set values_to_update = {values_to_update} of identify_name={identify_name} failed, because of {e}")
            return False

class ValidateParameters:
    def __init__(self):
        pass

    def is_in_ascii(self, s: str):
        return all(ord(char) < 127 and ord(char) > 31 for char in s)

    def is_file_ext(self, s:str, ext:str):
        _, file_ext = os.path.splitext(s)
        if file_ext == ext:
            return True
        else:
            return False

    def validate_type(self, request):
        try:
            assert isinstance(request.userID, str), "userID should be str"
            assert isinstance(request.taskID.taskID, str), "taskID should be str"
            # target
            for target_data_idx, target_data in enumerate(request.target.targetData):
                if isinstance(target_data.dataName, str):
                    data_name = target_data.dataName
                else:
                    raise AssertionError(f"The name of No.{target_data_idx} data in target should be str")
                assert isinstance(target_data.dataPath, str), f"data_name={data_name} dataPath should be str"
                assert isinstance(target_data.dataSplitType, bool), f"data_name={data_name} dataSplitType should be bool"
                assert isinstance(target_data.dataTransferType, int), f"data_name={data_name} dataTransferType should be int"
                assert isinstance(target_data.taskType, str), f"data_name={data_name} taskType should be str"
                if not all(isinstance(element, str) for element in target_data.totalSimulation.deviceTotalSimulation):
                    raise AssertionError(f"elements in data_name={data_name} totalSimulation.deviceTotalSimulation should be str")
                if not all(isinstance(element, int) for element in target_data.totalSimulation.numTotalSimulation):
                    raise AssertionError(f"elements in data_name={data_name} totalSimulation.numTotalSimulation should be int")
                if not all(isinstance(element, int) for element in target_data.totalSimulation.dynamicNumTotalSimulation):
                    raise AssertionError(f"elements in data_name={data_name} totalSimulation.dynamicNumTotalSimulation should be int")
                assert isinstance(target_data.allocation.optimization, bool), f"data_name={data_name} allocation.optimization should be bool"
                if not all(isinstance(element, int) for element in target_data.allocation.allocationLogicalSimulation):
                    raise AssertionError(f"elements in data_name={data_name} allocation.allocationLogicalSimulation should be int")
                if not all(isinstance(element, int) for element in target_data.allocation.allocationDeviceSimulation):
                    raise AssertionError(f"elements in data_name={data_name} allocation.allocationDeviceSimulation should be int")
                if not all(isinstance(element, str) for element in target_data.allocation.runningResponse.deviceRunningResponse):
                    raise AssertionError(f"elements in data_name={data_name} allocation.runningResponse.deviceRunningResponse should be str")
                if not all(isinstance(element, int) for element in target_data.allocation.runningResponse.numRunningResponse):
                    raise AssertionError(f"elements in data_name={data_name} allocation.runningResponse.numRunningResponse should be int")
            assert isinstance(request.target.priority, int), "target.priority should be int"

            # operatorFlow - flowSetting
            assert isinstance(request.operatorFlow.flowSetting.round, int), "operatorFlow.flowSetting.round should be int"
            assert isinstance(request.operatorFlow.flowSetting.startCondition.logicalSimulationStrategy.strategyCondition, str), "operatorFlow.flowSetting.startCondition.logicalSimulationStrategy.strategyCondition should be str"
            assert isinstance(request.operatorFlow.flowSetting.startCondition.logicalSimulationStrategy.waitInterval, int), "operatorFlow.flowSetting.startCondition.logicalSimulationStrategy.waitInterval should be int"
            assert isinstance(request.operatorFlow.flowSetting.startCondition.logicalSimulationStrategy.totalTimeout, int), "operatorFlow.flowSetting.startCondition.logicalSimulationStrategy.totalTimeout should be int"
            assert isinstance(request.operatorFlow.flowSetting.startCondition.deviceSimulationStrategy.strategyCondition, str), "operatorFlow.flowSetting.startCondition.deviceSimulationStrategy.strategyCondition should be str"
            assert isinstance(request.operatorFlow.flowSetting.startCondition.deviceSimulationStrategy.waitInterval, int), "operatorFlow.flowSetting.startCondition.deviceSimulationStrategy.waitInterval should be int"
            assert isinstance(request.operatorFlow.flowSetting.startCondition.deviceSimulationStrategy.totalTimeout, int), "operatorFlow.flowSetting.startCondition.deviceSimulationStrategy.totalTimeout should be int"
            assert isinstance(request.operatorFlow.flowSetting.stopCondition.logicalSimulationStrategy.strategyCondition, str), "operatorFlow.flowSetting.stopCondition.logicalSimulationStrategy.strategyCondition should be str"
            assert isinstance(request.operatorFlow.flowSetting.stopCondition.logicalSimulationStrategy.waitInterval, int), "operatorFlow.flowSetting.stopCondition.logicalSimulationStrategy.waitInterval should be int"
            assert isinstance(request.operatorFlow.flowSetting.stopCondition.logicalSimulationStrategy.totalTimeout, int), "operatorFlow.flowSetting.stopCondition.logicalSimulationStrategy.totalTimeout should be int"
            assert isinstance(request.operatorFlow.flowSetting.stopCondition.deviceSimulationStrategy.strategyCondition, str), "operatorFlow.flowSetting.stopCondition.deviceSimulationStrategy.strategyCondition should be str"
            assert isinstance(request.operatorFlow.flowSetting.stopCondition.deviceSimulationStrategy.waitInterval, int), "operatorFlow.flowSetting.stopCondition.deviceSimulationStrategy.waitInterval should be int"
            assert isinstance(request.operatorFlow.flowSetting.stopCondition.deviceSimulationStrategy.totalTimeout, int), "operatorFlow.flowSetting.stopCondition.deviceSimulationStrategy.totalTimeout should be int"

            # operatorFlow - operator
            for operator_idx, operator in enumerate(request.operatorFlow.operator):
                if isinstance(operator.name, str):
                    operator_name = operator.name
                else:
                    raise AssertionError(f"The name of No.{operator_idx} operator in operatorFlow.operator should be str")
                assert isinstance(operator.operationBehaviorController.useController, bool), f"operator={operator_name} operationBehaviorController.useController should be bool"
                assert isinstance(operator.operationBehaviorController.strategyBehaviorController, str), f"operator={operator_name} operationBehaviorController.strategyBehaviorController should be str"
                assert isinstance(operator.operationBehaviorController.outboundService, str), f"operator={operator_name} operationBehaviorController.outboundService should be str"

                # 针对input专门写一个校验，兼容传过来是""的情况
                if len(list(operator.input)) > 0:
                    if not all(isinstance(element, str) for element in operator.input):
                        raise AssertionError(f"elements in operator_name={operator_name} input should be str")

                assert isinstance(operator.useData, bool), f"operator={operator_name} useData should be bool"
                assert isinstance(operator.model.useModel, bool), f"operator={operator_name} model.useModel should be bool"
                assert isinstance(operator.model.modelForTrain, bool), f"operator={operator_name} model.modelForTrain should be bool"
                assert isinstance(operator.model.modelTransferType, int), f"operator={operator_name} model.modelTransferType should be int"
                assert isinstance(operator.model.modelPath, str), f"operator={operator_name} model.modelPath should be str"
                assert isinstance(operator.model.modelUpdateStyle, str), f"operator={operator_name} model.modelUpdateStyle should be str"
                assert isinstance(operator.logicalSimulationOperatorInfo.operatorTransferType, int), f"operator={operator_name} logicalSimulationOperatorInfo.operatorTransferType should be int"
                assert isinstance(operator.logicalSimulationOperatorInfo.operatorCodePath, str), f"operator={operator_name} logicalSimulationOperatorInfo.operatorCodePath should be str"
                assert isinstance(operator.logicalSimulationOperatorInfo.operatorEntryFile, str), f"operator={operator_name} logicalSimulationOperatorInfo.operatorEntryFile should be str"
                assert isinstance(operator.logicalSimulationOperatorInfo.operatorParams, str), f"operator={operator_name} logicalSimulationOperatorInfo.operatorParams should be str"
                assert isinstance(operator.deviceSimulationOperatorInfo.operatorTransferType, int), f"operator={operator_name} deviceSimulationOperatorInfo.operatorTransferType should be int"
                assert isinstance(operator.deviceSimulationOperatorInfo.operatorCodePath, str), f"operator={operator_name} deviceSimulationOperatorInfo.operatorCodePath should be str"
                assert isinstance(operator.deviceSimulationOperatorInfo.operatorEntryFile, str), f"operator={operator_name} deviceSimulationOperatorInfo.operatorEntryFile should be str"
                assert isinstance(operator.deviceSimulationOperatorInfo.operatorParams, str), f"operator={operator_name} deviceSimulationOperatorInfo.operatorParams should be str"

            # logicalSimulation
            if not all(isinstance(element, str) for element in request.logicalSimulation.computationUnit.devicesUnit):
                raise AssertionError(f"elements in logicalSimulation.computationUnit.devicesUnit should be str")
            if not all(isinstance(element.numCpus, int) for element in request.logicalSimulation.computationUnit.unitSetting):
                raise AssertionError(f"element.numCpus in logicalSimulation.computationUnit.unitSetting should be int")
            for resource_request_idx, resource_request in enumerate(request.logicalSimulation.resourceRequestLogicalSimulation):
                if isinstance(resource_request.dataNameResourceRequest, str):
                    resource_request_name = resource_request.dataNameResourceRequest
                else:
                    raise AssertionError(f"The name of No.{resource_request_idx} logicalSimulation.resourceRequestLogicalSimulation should be str")
                if not all(isinstance(element, str) for element in resource_request.deviceResourceRequest):
                    raise AssertionError(f"In logicalSimulation, elements in resource_request_name={resource_request_name} deviceResourceRequest should be str")
                if not all(isinstance(element, int) for element in resource_request.numResourceRequest):
                    raise AssertionError(f"In logicalSimulation, elements in resource_request_name={resource_request_name} numResourceRequest should be int")

            # deviceSimulation
            for resource_request_idx, resource_request in enumerate(request.deviceSimulation.resourceRequestDeviceSimulation):
                if isinstance(resource_request.dataNameResourceRequest, str):
                    resource_request_name = resource_request.dataNameResourceRequest
                else:
                    raise AssertionError(f"The name of No.{resource_request_idx} deviceSimulation.resourceRequestDeviceSimulation should be str")
                if not all(isinstance(element, str) for element in resource_request.deviceResourceRequest):
                    raise AssertionError(f"In deviceSimulation, elements in resource_request_name={resource_request_name} deviceResourceRequest should be str")
                if not all(isinstance(element, int) for element in resource_request.numResourceRequest):
                    raise AssertionError(f"In deviceSimulation, elements in resource_request_name={resource_request_name} numResourceRequest should be int")

            return True
        except Exception as e:
            try:
                task_id = request.taskID.taskID
                if not isinstance(task_id, str):
                    task_id = ""
            except:
                task_id = ""
            if isinstance(e, AssertionError):
                logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                             message=f"[validate_task_parameters] TypeError: {e}")
            else:
                logger.error(task_id=task_id, system_name="TaskMgr", module_name="task_manager",
                             message=f"[validate_task_parameters] {e}")
            return False

    def validate_correctness(self, request):
        try:
            assert request.userID != "", "userID should not be empty"
            assert self.is_in_ascii(request.userID), f"userID={request.userID} contains illegal characters. It should consist of English characters, symbols, and numbers."
            assert request.taskID.taskID != "", "taskID should not be empty"
            assert self.is_in_ascii(request.taskID.taskID), f"taskID={request.taskID.taskID} contains illegal characters. It should consist of English characters, symbols, and numbers."
            # target
            for target_data_idx, target_data in enumerate(request.target.targetData):
                assert target_data.dataName != "", f"The name of No.{target_data_idx} data in target should not be empty"
                assert self.is_in_ascii(target_data.dataName), f"The name of No.{target_data_idx} data in target = {target_data.dataName} contains illegal characters. It should consist of English characters, symbols, and numbers."
                data_name = target_data.dataName
                if target_data.dataPath: #不一定所有任务都需要数据路径，如果有，则需要判断
                    # assert self.is_file_ext(s=target_data.dataPath, ext=".zip"), f"target data_name = {data_name}, dataPath={target_data.dataPath} should be a file path with a .zip extension."
                    # dataPath可以是zip包路径或者是文件夹路径（文件夹路径只支持MINIO）
                    # pattern = re.compile(r'^[a-zA-Z0-9][\w/.-]*$')
                    pattern = re.compile(r'^[a-zA-Z0-9/._-]+$')
                    assert (self.is_file_ext(s=target_data.dataPath, ext=".zip") or bool(pattern.match(str(target_data.dataPath)))), \
                        f"target data_name = {data_name}, dataPath={target_data.dataPath} should be a .zip extension or a folder dir type."
                try:
                    taskService_pb2.FileTransferType.Name(target_data.dataTransferType)
                except:
                    raise AssertionError(f"The value of target_data.dataTransferType={target_data.dataTransferType} is not valid.")
                assert self.is_in_ascii(target_data.taskType), f"target data_name = {data_name}, target_data.taskType = {target_data.taskType} contains illegal characters. It should consist of English characters, symbols, and numbers."
                total_simulation_devices = list(target_data.totalSimulation.deviceTotalSimulation)
                if len(total_simulation_devices) == 0:
                    raise AssertionError(f"target data_name = {data_name}, totalSimulation.deviceTotalSimulation length should larger than 0")
                if len(total_simulation_devices) != len(set(total_simulation_devices)):
                    raise AssertionError(f"target data_name = {data_name}, totalSimulation.deviceTotalSimulation={total_simulation_devices} contains repeated elements!")
                if not all(self.is_in_ascii(element) for element in total_simulation_devices):
                    raise AssertionError(f"target data_name = {data_name}, elements in totalSimulation.deviceTotalSimulation={total_simulation_devices} contain illegal characters. It should consist of English characters, symbols, and numbers.")
                if not all(element > 0 for element in target_data.totalSimulation.numTotalSimulation):
                    raise AssertionError(f"target data_name = {data_name}, elements in totalSimulation.numTotalSimulation={list(target_data.totalSimulation.numTotalSimulation)} should be larger than 0.")
                if not all(element >= 0 for element in target_data.totalSimulation.dynamicNumTotalSimulation):
                    raise AssertionError(f"target data_name = {data_name}, elements in totalSimulation.dynamicNumTotalSimulation={list(target_data.totalSimulation.dynamicNumTotalSimulation)} should not be less than 0.")
                if not all(element >= 0 for element in target_data.allocation.allocationLogicalSimulation):
                    raise AssertionError(f"target data_name = {data_name}, elements in allocation.allocationLogicalSimulation={list(target_data.allocation.allocationLogicalSimulation)} should not be less than 0.")
                if not all(element >= 0 for element in target_data.allocation.allocationDeviceSimulation):
                    raise AssertionError(f"target data_name = {data_name}, elements in allocation.allocationDeviceSimulation={list(target_data.allocation.allocationDeviceSimulation)} should not be less than 0.")
                if not all(self.is_in_ascii(element) for element in target_data.allocation.runningResponse.deviceRunningResponse):
                    raise AssertionError(f"target data_name = {data_name}, elements in allocation.runningResponse.deviceRunningResponse={list(target_data.allocation.runningResponse.deviceRunningResponse)} contain illegal characters. It should consist of English characters, symbols, and numbers.")
                running_response_device = list(target_data.allocation.runningResponse.deviceRunningResponse)
                if len(running_response_device) != len(set(running_response_device)):
                    raise AssertionError(f"target data_name = {data_name}, runningResponse.deviceRunningResponse={running_response_device} contains repeated elements!")
                if not all(element >= 0 for element in target_data.allocation.runningResponse.numRunningResponse):
                    raise AssertionError(f"target data_name = {data_name}, elements in allocation.runningResponse.numRunningResponse={list(target_data.allocation.runningResponse.numRunningResponse)} should not be less than 0.")
            assert request.target.priority >= 0 and request.target.priority <= 10, f"target.priority={request.target.priority} should be in range from 0 to 10"

            # operatorFlow - flowSetting
            assert request.operatorFlow.flowSetting.round > 0, f"operatorFlow.flowSetting.round={request.operatorFlow.flowSetting.round} should be larger than 0"
            assert self.is_in_ascii(request.operatorFlow.flowSetting.startCondition.logicalSimulationStrategy.strategyCondition), f"operatorFlow.flowSetting.startCondition.logicalSimulationStrategy.strategyCondition = {request.operatorFlow.flowSetting.startCondition.logicalSimulationStrategy.strategyCondition} contains illegal characters. It should consist of English characters, symbols, and numbers."
            assert request.operatorFlow.flowSetting.startCondition.logicalSimulationStrategy.waitInterval >= 0, f"operatorFlow.flowSetting.startCondition.logicalSimulationStrategy.waitInterval = {request.operatorFlow.flowSetting.startCondition.logicalSimulationStrategy.waitInterval} should not be less than 0."
            assert request.operatorFlow.flowSetting.startCondition.logicalSimulationStrategy.totalTimeout >= 0, f"operatorFlow.flowSetting.startCondition.logicalSimulationStrategy.totalTimeout = {request.operatorFlow.flowSetting.startCondition.logicalSimulationStrategy.totalTimeout} should not be less than 0."
            assert self.is_in_ascii(request.operatorFlow.flowSetting.startCondition.deviceSimulationStrategy.strategyCondition), f"operatorFlow.flowSetting.startCondition.deviceSimulationStrategy.strategyCondition = {request.operatorFlow.flowSetting.startCondition.deviceSimulationStrategy.strategyCondition} contains illegal characters. It should consist of English characters, symbols, and numbers."
            assert request.operatorFlow.flowSetting.startCondition.deviceSimulationStrategy.waitInterval >= 0, f"operatorFlow.flowSetting.startCondition.deviceSimulationStrategy.waitInterval = {request.operatorFlow.flowSetting.startCondition.deviceSimulationStrategy.waitInterval} should not be less than 0."
            assert request.operatorFlow.flowSetting.startCondition.deviceSimulationStrategy.totalTimeout >= 0, f"operatorFlow.flowSetting.startCondition.deviceSimulationStrategy.totalTimeout = {request.operatorFlow.flowSetting.startCondition.deviceSimulationStrategy.totalTimeout} should not be less than 0."
            assert self.is_in_ascii(request.operatorFlow.flowSetting.stopCondition.logicalSimulationStrategy.strategyCondition), f"operatorFlow.flowSetting.stopCondition.logicalSimulationStrategy.strategyCondition = {request.operatorFlow.flowSetting.stopCondition.logicalSimulationStrategy.strategyCondition} contains illegal characters. It should consist of English characters, symbols, and numbers."
            assert request.operatorFlow.flowSetting.stopCondition.logicalSimulationStrategy.waitInterval >= 0, f"operatorFlow.flowSetting.stopCondition.logicalSimulationStrategy.waitInterval = {request.operatorFlow.flowSetting.stopCondition.logicalSimulationStrategy.waitInterval} should not be less than 0."
            assert request.operatorFlow.flowSetting.stopCondition.logicalSimulationStrategy.totalTimeout >= 0, f"operatorFlow.flowSetting.stopCondition.logicalSimulationStrategy.totalTimeout = {request.operatorFlow.flowSetting.stopCondition.logicalSimulationStrategy.totalTimeout} should not be less than 0."
            assert self.is_in_ascii(request.operatorFlow.flowSetting.stopCondition.deviceSimulationStrategy.strategyCondition), f"operatorFlow.flowSetting.stopCondition.deviceSimulationStrategy.strategyCondition = {request.operatorFlow.flowSetting.stopCondition.deviceSimulationStrategy.strategyCondition} contains illegal characters. It should consist of English characters, symbols, and numbers."
            assert request.operatorFlow.flowSetting.stopCondition.deviceSimulationStrategy.waitInterval >= 0, f"operatorFlow.flowSetting.stopCondition.deviceSimulationStrategy.waitInterval = {request.operatorFlow.flowSetting.stopCondition.deviceSimulationStrategy.waitInterval} should not be less than 0."
            assert request.operatorFlow.flowSetting.stopCondition.deviceSimulationStrategy.totalTimeout >= 0, f"operatorFlow.flowSetting.stopCondition.deviceSimulationStrategy.totalTimeout = {request.operatorFlow.flowSetting.stopCondition.deviceSimulationStrategy.totalTimeout} should not be less than 0."

            # operatorFlow - operator
            for operator_idx, operator in enumerate(request.operatorFlow.operator):
                assert operator.name != "", f"The name of No.{operator_idx} operator in operatorFlow should not be empty"
                assert self.is_in_ascii(operator.name), f"The name of No.{operator_idx} operator in operatorFlow = {operator.name} contains illegal characters. It should consist of English characters, symbols, and numbers."
                # 补充算子名中不能有空格
                assert " " not in operator.name, f"The name of No.{operator_idx} operator in operatorFlow = {operator.name} includes spaces, and that's not allowed."
                operator_name = operator.name
                assert self.is_in_ascii(operator.operationBehaviorController.strategyBehaviorController), f"operator={operator_name} operationBehaviorController.strategyBehaviorController={operator.operationBehaviorController.strategyBehaviorController} contains illegal characters. It should consist of English characters, symbols, and numbers."
                assert self.is_in_ascii(operator.operationBehaviorController.outboundService), f"operator={operator_name} operationBehaviorController.outboundService={operator.operationBehaviorController.outboundService} contains illegal characters. It should consist of English characters, symbols, and numbers."
                if not all(self.is_in_ascii(element) for element in operator.input):
                    raise AssertionError(f"elements in operator_name={operator_name} input = {list(operator.input)} contain illegal characters. It should consist of English characters, symbols, and numbers.")
                try:
                    taskService_pb2.FileTransferType.Name(operator.model.modelTransferType)
                except:
                    raise AssertionError(f"operator_name={operator_name}, the value of operator.model.modelTransferType={operator.model.modelTransferType} is not valid.")
                assert self.is_in_ascii(operator.model.modelPath), f"operator_name={operator_name}, operator.model.modelPath={operator.model.modelPath} contains illegal characters. It should consist of English characters, symbols, and numbers."
                assert self.is_in_ascii(operator.model.modelUpdateStyle), f"operator_name={operator_name}, operator.model.modelUpdateStyle={operator.model.modelUpdateStyle} contains illegal characters. It should consist of English characters, symbols, and numbers."
                # operator - logicalSimulation
                try:
                    taskService_pb2.FileTransferType.Name(operator.logicalSimulationOperatorInfo.operatorTransferType)
                except:
                    raise AssertionError(f"operator_name={operator_name}, the value of logicalSimulationOperatorInfo.operatorTransferType={operator.logicalSimulationOperatorInfo.operatorTransferType} is not valid.")
                if operator.logicalSimulationOperatorInfo.operatorCodePath != "":
                    assert self.is_in_ascii(operator.logicalSimulationOperatorInfo.operatorCodePath), f"operator.logicalSimulationOperatorInfo.operatorCodePath = {operator.logicalSimulationOperatorInfo.operatorCodePath} contains illegal characters. It should consist of English characters, symbols, and numbers."
                    if not (os.path.isdir(os.path.abspath(operator.logicalSimulationOperatorInfo.operatorCodePath)) or self.is_file_ext(s=operator.logicalSimulationOperatorInfo.operatorCodePath, ext=".zip")):
                        raise AssertionError(f"operator.logicalSimulationOperatorInfo.operatorCodePath of {operator_name} = {operator.logicalSimulationOperatorInfo.operatorCodePath} should be an existing directory or a file path with a .zip extension.")
                if operator.logicalSimulationOperatorInfo.operatorEntryFile != "":
                    assert self.is_in_ascii(operator.logicalSimulationOperatorInfo.operatorEntryFile), f"operator.logicalSimulationOperatorInfo.operatorEntryFile = {operator.logicalSimulationOperatorInfo.operatorEntryFile} contains illegal characters. It should consist of English characters, symbols, and numbers."
                    assert self.is_file_ext(s=operator.logicalSimulationOperatorInfo.operatorEntryFile, ext=".py"), f"operator.logicalSimulationOperatorInfo.operatorEntryFile of {operator_name} = {operator.logicalSimulationOperatorInfo.operatorEntryFile} should be a file path with a .py extension."
                if operator.logicalSimulationOperatorInfo.operatorParams:  # 如果具有算子参数，需要判断是否为json形式
                    try:
                        json.loads(operator.logicalSimulationOperatorInfo.operatorParams)
                    except:
                        raise AssertionError(f"operator={operator_name} logicalSimulationOperatorInfo.operatorParams should be a json string")
                # operator - deviceSimulation
                try:
                    taskService_pb2.FileTransferType.Name(operator.deviceSimulationOperatorInfo.operatorTransferType)
                except:
                    raise AssertionError(f"operator_name={operator_name}, the value of deviceSimulationOperatorInfo.operatorTransferType={operator.deviceSimulationOperatorInfo.operatorTransferType} is not valid.")
                if operator.deviceSimulationOperatorInfo.operatorCodePath != "":
                    assert self.is_in_ascii(operator.deviceSimulationOperatorInfo.operatorCodePath), f"operator.deviceSimulationOperatorInfo.operatorCodePath = {operator.deviceSimulationOperatorInfo.operatorCodePath} contains illegal characters. It should consist of English characters, symbols, and numbers."
                    # if not (self.is_file_ext(s=operator.deviceSimulationOperatorInfo.operatorCodePath, ext=".zip") or self.is_file_ext(s=operator.deviceSimulationOperatorInfo.operatorCodePath, ext=".apk")):
                    #     raise AssertionError(f"operator.deviceSimulationOperatorInfo.operatorCodePath of {operator_name} = {operator.deviceSimulationOperatorInfo.operatorCodePath} should be a file path with a .zip or .apk extension.")
                    if not self.is_file_ext(s=operator.deviceSimulationOperatorInfo.operatorCodePath, ext=".apk"):
                        raise AssertionError(f"operator.deviceSimulationOperatorInfo.operatorCodePath of {operator_name} = {operator.deviceSimulationOperatorInfo.operatorCodePath} should be a file path with a .zip or .apk extension.")
                if operator.deviceSimulationOperatorInfo.operatorEntryFile != "":
                    assert self.is_in_ascii(operator.deviceSimulationOperatorInfo.operatorEntryFile), f"operator.deviceSimulationOperatorInfo.operatorEntryFile = {operator.deviceSimulationOperatorInfo.operatorEntryFile} contains illegal characters. It should consist of English characters, symbols, and numbers."
                    assert self.is_file_ext(s=operator.deviceSimulationOperatorInfo.operatorEntryFile, ext=".apk"), f"operator.deviceSimulationOperatorInfo.operatorEntryFile of {operator_name} = {operator.deviceSimulationOperatorInfo.operatorEntryFile} should be a file path with a .apk extension."
                if operator.deviceSimulationOperatorInfo.operatorParams:  # 如果具有算子参数，需要判断是否为json形式
                    try:
                        json.loads(operator.deviceSimulationOperatorInfo.operatorParams)
                    except:
                        raise AssertionError(f"operator={operator_name} deviceSimulationOperatorInfo.operatorParams should be a json string")

            # logicalSimulation
            logical_unit_devices = list(request.logicalSimulation.computationUnit.devicesUnit)
            if len(logical_unit_devices) != len(set(logical_unit_devices)):
                raise AssertionError(f"logicalSimulation, computationUnit.devicesUnit={logical_unit_devices} contains repeated elements!")
            if not all(self.is_in_ascii(element) for element in logical_unit_devices):
                raise AssertionError(f"logicalSimulation, elements in computationUnit.devicesUnit={logical_unit_devices} contain illegal characters. It should consist of English characters, symbols, and numbers.")
            if not all(element.numCpus >= 1 for element in request.logicalSimulation.computationUnit.unitSetting):
                raise AssertionError(f"element.numCpus in logicalSimulation.computationUnit.unitSetting should not be less than 1.")
            for resource_request_idx, resource_request in enumerate(request.logicalSimulation.resourceRequestLogicalSimulation):
                assert resource_request.dataNameResourceRequest != "", f"The name of No.{resource_request_idx} resource_request in logicalSimulation should not be empty"
                assert self.is_in_ascii(resource_request.dataNameResourceRequest), f"The name of No.{resource_request_idx} resource_request in logicalSimulation = {resource_request.dataNameResourceRequest} contains illegal characters. It should consist of English characters, symbols, and numbers."
                resource_request_name = resource_request.dataNameResourceRequest
                resource_request_devices = list(resource_request.deviceResourceRequest)
                if len(resource_request_devices) != len(set(resource_request_devices)):
                    raise AssertionError(f"In logicalSimulation, resource_request.name = {resource_request_name} deviceResourceRequest = {resource_request_devices} contains repeated elements!")
                if not all(self.is_in_ascii(element) for element in resource_request_devices):
                    raise AssertionError(f"In logicalSimulation, elements in resource_request.name={resource_request_name} deviceResourceRequest = {resource_request_devices} contain illegal characters. It should consist of English characters, symbols, and numbers.")
                if not all(element >= 0 for element in resource_request.numResourceRequest):
                    raise AssertionError(f"In logicalSimulation, elements in resource_request_name={resource_request_name} numResourceRequest should not be less than 0.")

            # deviceSimulation
            for resource_request_idx, resource_request in enumerate(request.deviceSimulation.resourceRequestDeviceSimulation):
                assert resource_request.dataNameResourceRequest != "", f"The name of No.{resource_request_idx} resource_request in deviceSimulation should not be empty"
                assert self.is_in_ascii(resource_request.dataNameResourceRequest), f"The name of No.{resource_request_idx} resource_request in deviceSimulation = {resource_request.dataNameResourceRequest} contains illegal characters. It should consist of English characters, symbols, and numbers."
                resource_request_name = resource_request.dataNameResourceRequest
                resource_request_devices = list(resource_request.deviceResourceRequest)
                if len(resource_request_devices) != len(set(resource_request_devices)):
                    raise AssertionError(f"In deviceSimulation, resource_request.name = {resource_request_name} deviceResourceRequest = {resource_request_devices} contains repeated elements!")
                if not all(self.is_in_ascii(element) for element in resource_request_devices):
                    raise AssertionError(f"In deviceSimulation, elements in resource_request.name={resource_request_name} deviceResourceRequest = {resource_request_devices} contain illegal characters. It should consist of English characters, symbols, and numbers.")
                if not all(element >= 0 for element in resource_request.numResourceRequest):
                    raise AssertionError(f"In logicalSimulation, elements in resource_request_name={resource_request_name} numResourceRequest should not be less than 0.")

            return True
        except Exception as e:
            logger.error(task_id=request.taskID.taskID, system_name="TaskMgr", module_name="task_manager",
                         message=f"[validate_task_parameters] ValueError: {e}")
            return False

    def validate_relationship(self, request):
        try:
            data_names_total_simulation = []
            # target
            for target_data_idx, target_data in enumerate(request.target.targetData):
                data_name = target_data.dataName
                data_names_total_simulation.append(data_name)

                # dataPath的校验
                if target_data.dataPath:
                    file_tranfer_type = taskService_pb2.FileTransferType.Name(target_data.dataTransferType)
                    if file_tranfer_type != "MINIO" and file_tranfer_type != "FILE":
                        assert (self.is_file_ext(s=target_data.dataPath, ext=".zip")), \
                                f"target, data_name={data_name}, file_tranfer_type = {file_tranfer_type}, dataPath={target_data.dataPath} should be a file path with a .zip extension."

                # totalSimulation里面的devices, nums, dynamic_nums的维度相同
                devices = list(target_data.totalSimulation.deviceTotalSimulation)
                nums = list(target_data.totalSimulation.numTotalSimulation)
                dynamic_nums = list(target_data.totalSimulation.dynamicNumTotalSimulation)
                assert (len(devices) == len(nums)) and (len(nums) == len(dynamic_nums)), f"target, data_name={data_name}, devices, nums and dynamic_nums in total_simulation should have the same length"
                # totalSimulation里面的nums需要大于dynamic_nums
                assert all(nums[i] > dynamic_nums[i] for i in range(len(nums))), f"target, data_name={data_name}, elements in nums={nums} should be larger than dynamic_nums={dynamic_nums}"
                # runningResponse里面的devices, nums的维度相同
                rr_devices = list(target_data.allocation.runningResponse.deviceRunningResponse)
                # 需要判断rr_devices里的元素均为totalSimulation里的元素
                assert set(rr_devices).issubset(devices), (
                    f"target, data_name={data_name}, rr_devices = {list(rr_devices)} should be in total simulation devices = {devices}"
                )
                rr_nums = list(target_data.allocation.runningResponse.numRunningResponse)
                assert len(rr_devices) == len(rr_nums), f"target, data_name={data_name}, devices, nums in allocation.runningResponse.numRunningResponse should have the same length"
                # 仿真数量的比较
                map_rr = {rr_device: rr_number for rr_device, rr_number in zip(rr_devices, rr_nums)}
                rr_nums_reorder = [map_rr.get(total_device, 0) for total_device in devices]

                # # 仿真数量的比较: runningResponse里面的nums不能大于totalSimulation里面的nums
                # assert all(rr_nums_reorder[i] <= nums[i] for i in range(len(nums))), f"target, data_name={data_name}, nums in runningResponse ({rr_nums_reorder}) should not be larger than nums in totalSimulation ({nums})."
                # # 仿真数量的比较: 如果allocation.optimization是false，则需要保证一个data内：不同devices的机次数分配+待测量设备数 = 任务设定总数
                # if target_data.allocation.optimization == False:
                #     allocation_logical_nums = list(target_data.allocation.allocationLogicalSimulation)
                #     allocation_device_nums = list(target_data.allocation.allocationDeviceSimulation)
                #     if allocation_logical_nums == []:
                #         allocation_logical_nums = [0] * len(nums)
                #     if allocation_device_nums == []:
                #         allocation_device_nums = [0] * len(nums)
                #     assert len(allocation_logical_nums) == len(nums) and len(allocation_logical_nums) == len(allocation_device_nums), \
                #         f"target, data_name={data_name}, nums={nums}, allocation_logical_nums={allocation_logical_nums}, allocation_device_nums={allocation_device_nums} should have the same length"
                #     assert all(nums[i] == allocation_logical_nums[i] + allocation_device_nums[i] + rr_nums_reorder[i] for i in range(len(nums))), \
                #         f"target, data_name={data_name}, the number of allocation and running response should equal to total simulation."

                # 2024-10-23更改逻辑，真机仿真机次数 = 真机仿真分配数 + 真机测量机次数量
                # 仿真数量的比较: runningResponse里面的nums不能大于totalSimulation里面的nums
                assert all(rr_nums_reorder[i] <= nums[i] for i in range(len(nums))), f"target, data_name={data_name}, nums in runningResponse ({rr_nums_reorder}) should not be larger than nums in totalSimulation ({nums})."
                # 仿真数量的比较: 如果allocation.optimization是false，则需要保证一个data内：任务设定总数 = 不同devices的机次数分配 >= 待测量设备数
                if target_data.allocation.optimization == False:
                    allocation_logical_nums = list(target_data.allocation.allocationLogicalSimulation)
                    allocation_device_nums = list(target_data.allocation.allocationDeviceSimulation)
                    if allocation_logical_nums == []:
                        allocation_logical_nums = [0] * len(nums)
                    if allocation_device_nums == []:
                        allocation_device_nums = [0] * len(nums)
                    assert len(allocation_logical_nums) == len(nums) and len(allocation_logical_nums) == len(allocation_device_nums), \
                        f"target, data_name={data_name}, nums={nums}, allocation_logical_nums={allocation_logical_nums}, allocation_device_nums={allocation_device_nums} should have the same length"
                    assert all(nums[i] == allocation_logical_nums[i] + allocation_device_nums[i] for i in range(len(nums))), \
                        f"target, data_name={data_name}, the number of allocation  should equal to total simulation."
                    assert all(allocation_device_nums[i] >= rr_nums_reorder[i] for i in range(len(nums))), \
                        f"target, data_name={data_name}, the number of device allocation should no less than running response."


            # operatorFlow - flowSetting
            # waitInterval不大于totalTimeout
            start_logical_waitInterval = request.operatorFlow.flowSetting.startCondition.logicalSimulationStrategy.waitInterval
            start_logical_totalTimeout = request.operatorFlow.flowSetting.startCondition.logicalSimulationStrategy.totalTimeout
            start_device_waitInterval = request.operatorFlow.flowSetting.startCondition.deviceSimulationStrategy.waitInterval
            start_device_totalTimeout = request.operatorFlow.flowSetting.startCondition.deviceSimulationStrategy.totalTimeout
            stop_logical_waitInterval = request.operatorFlow.flowSetting.stopCondition.logicalSimulationStrategy.waitInterval
            stop_logical_totalTimeout = request.operatorFlow.flowSetting.stopCondition.logicalSimulationStrategy.totalTimeout
            stop_device_waitInterval = request.operatorFlow.flowSetting.stopCondition.deviceSimulationStrategy.waitInterval
            stop_device_totalTimeout = request.operatorFlow.flowSetting.stopCondition.deviceSimulationStrategy.totalTimeout
            assert (start_logical_waitInterval <= start_logical_totalTimeout and start_device_waitInterval <= start_device_totalTimeout
                    and stop_logical_waitInterval <= stop_logical_totalTimeout and stop_device_waitInterval <= stop_device_totalTimeout), \
                f"waitInterval in operatorflow should be no larger than totalTimeout."

            operator_names_list = []
            # operatorFlow - operator
            for operator_idx, operator in enumerate(request.operatorFlow.operator):
                operator_name = operator.name

                # 使用梯度中间站(DeviceFlow)时，策略不能为空
                if operator.operationBehaviorController.useController:
                    assert operator.operationBehaviorController.strategyBehaviorController != "", f"operator_name = {operator_name}, operationBehaviorController.strategyBehaviorController should not be empty when useController is True"

                # 如果有input, 则需要判断input是否在之前的算子中，否则需要报错
                if list(operator.input):
                    assert set(list(operator.input)).issubset(set(operator_names_list)), (
                        f"operator_name = {operator_name}, operator.input = {list(operator.input)} should be in previous operator list"
                    )

                # 如果使用模型，model_path不能为空
                if operator.model.useModel:
                    assert operator.model.modelPath != "", f"operator_name = {operator_name}, modelPath should not be empty when useModel == True"
                    # # 如果model_for_train, model_update_style不能为空(现在为空也可以)
                    # if operator.model.modelForTrain:
                    #     assert operator.model.modelUpdateStyle != "", f"operator_name = {operator_name}, modelUpdateStyle should not be empty when modelForTrain == True"

                # logical_simulation
                # 如果operatorCodePath是dir，只允许FileTransferType为FILE
                operator_code_path = operator.logicalSimulationOperatorInfo.operatorCodePath
                if operator_code_path != "":
                    if os.path.isdir(os.path.abspath(operator_code_path)):
                        assert taskService_pb2.FileTransferType.Name(operator.logicalSimulationOperatorInfo.operatorTransferType) == "FILE", f"operator={operator_name}, if operatorCodePath is a dir, only FILE is allowed for FileTransferType"

                # logical_simulation 和 device_simulation里，operator_code_path、operator_entry_file不能同时为空
                if (operator.logicalSimulationOperatorInfo.operatorCodePath == "" and
                    operator.deviceSimulationOperatorInfo.operatorCodePath == ""):
                    raise AssertionError(f"operator_name = {operator_name}, operatorCodePath in logical_simulation and device_simulation should not be empty")
                if (operator.logicalSimulationOperatorInfo.operatorEntryFile == "" and
                    operator.deviceSimulationOperatorInfo.operatorEntryFile == ""):
                    raise AssertionError(f"operator_name = {operator_name}, operatorEntryFile in logical_simulation and device_simulation should not be empty")

                operator_names_list.append(operator_name)

            # 综合判断，target里面的data名的数量，应等于logicalSimulation和deviceSimulation的resource_request的data名数量
            target_data_name = []
            for target_data in request.target.targetData:
                target_data_name.append(target_data.dataName)
            resource_request_data_name = []
            for resource_request_logical in request.logicalSimulation.resourceRequestLogicalSimulation:
                resource_request_data_name.append(resource_request_logical.dataNameResourceRequest)
            for resource_request_device in request.deviceSimulation.resourceRequestDeviceSimulation:
                resource_request_data_name.append(resource_request_device.dataNameResourceRequest)
            if not (set(target_data_name).issubset(set(resource_request_data_name)) and
                    set(resource_request_data_name).issubset(set(target_data_name))):
                raise AssertionError(f"resource request in both logical simulation and device simulation should cover the target data.")

            # logicalSimulation
            # 计算单元
            # 设备类型和setting长度需相同
            logical_unit_devices = list(request.logicalSimulation.computationUnit.devicesUnit)
            computation_unit_setting = list(request.logicalSimulation.computationUnit.unitSetting)
            assert len(logical_unit_devices) == len(computation_unit_setting), \
                f"logicalSimulation, computationUnit.devicesUnit={logical_unit_devices} and computationUnit.unitSetting = {computation_unit_setting} should have the same length!"
            # 所有数据total_simulation设备集合应为计算单元的设备类型元素的子集
            all_total_devices = []
            for target_data_idx, target_data in enumerate(request.target.targetData):
                devices = list(target_data.totalSimulation.deviceTotalSimulation)
                all_total_devices.extend(devices)
            if not set(all_total_devices).issubset(set(logical_unit_devices)):
                raise AssertionError(f"In logicalSimulation, all devices within all_total_devices = {all_total_devices} should correspond to the devices included in computationUnit.devicesUnit={logical_unit_devices}.")
            # num_request对应项的最小值应大于0 (除非optimization为False且allocation的对应项为0)
            for resource_request in request.logicalSimulation.resourceRequestLogicalSimulation:
                resource_request_name = resource_request.dataNameResourceRequest
                assert resource_request_name in data_names_total_simulation, (
                    f"In logicalSimulation, data_name = {resource_request_name} should be in total_simulation = {data_names_total_simulation}"
                )
                resource_request_devices = list(resource_request.deviceResourceRequest)
                resource_request_num = list(resource_request.numResourceRequest)
                # 判断resource_request_devices和resource_request_num的长度是否相同
                assert len(resource_request_devices) == len(resource_request_num), (
                    f"In logicalSimulation, data_name = {resource_request_name}, resource_request_devices = {resource_request_devices} should have the same length of resource_request_num = {resource_request_num}"
                )
                map_resource_request = {request_device: request_num for request_device, request_num in zip(resource_request_devices, resource_request_num)}
                for target_data in request.target.targetData:
                    data_name = target_data.dataName
                    if resource_request_name == data_name:
                        if target_data.allocation.optimization == False:
                            total_simulation_devices = list(target_data.totalSimulation.deviceTotalSimulation)
                            allocation_logical_nums = list(target_data.allocation.allocationLogicalSimulation)
                            map_allocation_logical = {
                                simulation_device: allocation_devices_num
                                for simulation_device, allocation_devices_num in zip(
                                    total_simulation_devices, allocation_logical_nums
                                )
                            }
                        else:
                            map_allocation_logical = {}
                        for device_key in map_resource_request:
                            num_request_logical = map_resource_request.get(device_key)
                            num_allocation_logical = map_allocation_logical.get(device_key, 0)
                            if target_data.allocation.optimization == False:
                                if num_allocation_logical == 0:
                                    assert num_request_logical >= 0, (
                                        f"In logicalSimulation, data_name = {data_name}, num_allocation_logical = {num_allocation_logical} should be no less than 0."
                                    )
                                else:
                                    assert num_request_logical > 0, (
                                        f"In logicalSimulation, data_name = {data_name}, num_request_logical = {num_request_logical} should be larger than 0 while target_data.allocation.optimization == False."
                                    )
                            else:
                                assert num_request_logical >= 0, (
                                    f"In logicalSimulation, data_name = {data_name}, num_request_logical = {num_request_logical} should be no less than 0 while target_data.allocation.optimization == True."
                                )
                        # break


            # deviceSimulation
            # 2024-10-23逻辑改动，device_simulation对应项 包含 running_response.nums
            # num_request对应项的最小值应该为running_response.nums对应项+1（如果device_simulation对应项大于running_response.nums对应项）
            for resource_request in request.deviceSimulation.resourceRequestDeviceSimulation:
                resource_request_name = resource_request.dataNameResourceRequest
                assert resource_request_name in data_names_total_simulation, (
                    f"In deviceSimulation, data_name = {resource_request_name} should be in total_simulation = {data_names_total_simulation}"
                )
                resource_request_devices = list(resource_request.deviceResourceRequest)
                resource_request_num = list(resource_request.numResourceRequest)
                # 判断resource_request_devices和resource_request_num的长度是否相同
                assert len(resource_request_devices) == len(resource_request_num), (
                    f"In deviceSimulation, data_name = {resource_request_name}, resource_request_devices = {resource_request_devices} should have the same length of resource_request_num = {resource_request_num}"
                )
                map_resource_request = {request_device: request_num for request_device, request_num in zip(resource_request_devices, resource_request_num)}
                for target_data in request.target.targetData:
                    data_name = target_data.dataName
                    if resource_request_name == data_name:
                        rr_devices = list(target_data.allocation.runningResponse.deviceRunningResponse)
                        rr_nums = list(target_data.allocation.runningResponse.numRunningResponse)
                        map_rr = {rr_device: rr_number for rr_device, rr_number in zip(rr_devices, rr_nums)}
                        if target_data.allocation.optimization == False:
                            total_simulation_devices = list(target_data.totalSimulation.deviceTotalSimulation)
                            allocation_devices_nums = list(target_data.allocation.allocationDeviceSimulation)
                            map_allocation_devices = {
                                simulation_device: allocation_devices_num
                                for simulation_device, allocation_devices_num in zip(
                                    total_simulation_devices, allocation_devices_nums
                                )
                            }
                        else:
                            map_allocation_devices = {}
                        # 逐个数据比较
                        if target_data.allocation.optimization == False:
                            for device_key in map_allocation_devices:
                                num_request_device = map_resource_request.get(device_key, 0)
                                num_running_response = map_rr.get(device_key, 0)
                                num_allocation_device = map_allocation_devices.get(device_key, 0)
                                if num_allocation_device == num_running_response: # 2024-10-23修改
                                    assert num_request_device >= num_running_response, (
                                        f"In deviceSimulation, data_name = {data_name}, num_request_device = {num_request_device} should be no less than num_running_response = {num_running_response} while num_allocation_device == 0."
                                    )
                                else:
                                    assert num_request_device >= 1, f"In deviceSimulation, data_name = {data_name}, num_request_device = {num_request_device} should be larger than 1 when num_allocation_device of {device_key} > 0."
                                    assert num_request_device > num_running_response, (
                                        f"In deviceSimulation, data_name = {data_name}, num_request_device = {num_request_device} should be larger than num_running_response = {num_running_response}!"
                                    )
                        else:
                            for device_key in map_resource_request:
                                num_request_device = map_resource_request.get(device_key)
                                num_running_response = map_rr.get(device_key, 0)
                                if num_running_response > 0: #在num_running_response>0的时候才做校验
                                    assert num_request_device > num_running_response, (
                                        f"In deviceSimulation, data_name = {data_name}, num_request_device = {num_request_device} should be larger than num_running_response = {num_running_response} while target_data.allocation.optimization == True."
                                    )
                        # break

            return True
        except Exception as e:
            logger.error(task_id=request.taskID.taskID, system_name="TaskMgr", module_name="task_manager",
                         message=f"[validate_task_parameters] RelationError: {e}")
        return False

    def validate_task_parameters(self, request):
        '''
        参数校验，确认参数是否符合约束条件
        '''
        # 参数是否存在及类型验证
        validate_type_result = self.validate_type(request)
        if validate_type_result == False:
            return False
        # 参数值正确性判断
        validate_correctness_result = self.validate_correctness(request)
        if validate_correctness_result == False:
            return False
        # 参数关系判断
        validate_relationship_result = self.validate_relationship(request)
        if validate_relationship_result == False:
            return False
        return True

def json2taskconfig(jsonstring):
    jsondata = json.loads(jsonstring)

    # target
    target_json = jsondata.get("target", {})
    data_json_list = target_json.get("data", [])
    target_data_list = []
    for data_index, data_json in enumerate(data_json_list):
        total_simulation_json = data_json.get("total_simulation", {})
        total_simulation = taskService_pb2.TotalSimulation(
            deviceTotalSimulation = total_simulation_json.get("devices", []),
            numTotalSimulation = total_simulation_json.get("nums", []),
            dynamicNumTotalSimulation = total_simulation_json.get("dynamic_nums", [])
        )
        allocation_json = data_json.get("allocation", {})
        running_response_json = allocation_json.get("running_response", {})
        running_response = taskService_pb2.RunningResponse(
            deviceRunningResponse = running_response_json.get("devices", []),
            numRunningResponse = running_response_json.get("nums", [])
        )
        allocation = taskService_pb2.Allocation(
            optimization = allocation_json.get("optimization", False),
            allocationLogicalSimulation = allocation_json.get("logical_simulation", []),
            allocationDeviceSimulation = allocation_json.get("device_simulation", []),
            runningResponse = running_response
        )
        data_transfer_type_str = data_json.get("data_transfer_type", "S3")
        target_data = taskService_pb2.TargetData(
            dataName = data_json.get("name", f"data_{data_index}"),
            dataPath = data_json.get("data_path", ""),
            dataSplitType = data_json.get("data_split_type", False),
            dataTransferType = eval(f"taskService_pb2.FileTransferType.{data_transfer_type_str}"),
            taskType = data_json.get("task_type", ""),
            totalSimulation = total_simulation,
            allocation = allocation
        )
        target_data_list.append(target_data)
    target = taskService_pb2.Target(
        targetData = target_data_list,
        priority = target_json.get("priority", 0)
    )

    # operatorflow
    operatorflow_json = jsondata.get("operatorflow", {})
    flow_setting_json = operatorflow_json.get("flow_setting", {})
    operator_list_json = operatorflow_json.get("operators", [])
    # operatorflow - flow_setting - start
    start_strategy_json = flow_setting_json.get("start", {})
    start_strategy_logical_simulation_json = start_strategy_json.get("logical_simulation", {})
    start_strategy_device_simulation_json = start_strategy_json.get("device_simulation", {})
    start_strategy_logical_simulation = taskService_pb2.StrategyCondition(
        strategyCondition = start_strategy_logical_simulation_json.get("strategy", ""),
        waitInterval = start_strategy_logical_simulation_json.get("wait_interval", 0),
        totalTimeout = start_strategy_logical_simulation_json.get("total_timeout", 0)
    )
    start_strategy_device_simulation = taskService_pb2.StrategyCondition(
        strategyCondition = start_strategy_device_simulation_json.get("strategy", ""),
        waitInterval = start_strategy_device_simulation_json.get("wait_interval", 0),
        totalTimeout = start_strategy_device_simulation_json.get("total_timeout", 0)
    )
    start_condition = taskService_pb2.OperatorFlowCondition(
        logicalSimulationStrategy = start_strategy_logical_simulation,
        deviceSimulationStrategy = start_strategy_device_simulation
    )
    # operatorflow - flow_setting - stop
    stop_strategy_json = flow_setting_json.get("stop", {})
    stop_strategy_logical_simulation_json = stop_strategy_json.get("logical_simulation", {})
    stop_strategy_device_simulation_json = stop_strategy_json.get("device_simulation", {})
    stop_strategy_logical_simulation = taskService_pb2.StrategyCondition(
        strategyCondition = stop_strategy_logical_simulation_json.get("strategy", ""),
        waitInterval = stop_strategy_logical_simulation_json.get("wait_interval", 0),
        totalTimeout = stop_strategy_logical_simulation_json.get("total_timeout", 0)
    )
    stop_strategy_device_simulation = taskService_pb2.StrategyCondition(
        strategyCondition = stop_strategy_device_simulation_json.get("strategy", ""),
        waitInterval = stop_strategy_device_simulation_json.get("wait_interval", 0),
        totalTimeout = stop_strategy_device_simulation_json.get("total_timeout", 0)
    )
    stop_condition = taskService_pb2.OperatorFlowCondition(
        logicalSimulationStrategy = stop_strategy_logical_simulation,
        deviceSimulationStrategy = stop_strategy_device_simulation
    )
    flow_setting = taskService_pb2.FlowSetting(
        round = flow_setting_json.get("round", 0),
        startCondition = start_condition,
        stopCondition = stop_condition
    )
    # operatorflow - operator
    operator_list = []
    for operator_json in operator_list_json:
        # operation_behavior_controller
        operation_behavior_controller_json = operator_json.get("operation_behavior_controller", {})
        operation_behavior_controller = taskService_pb2.OperationBehaviorController(
            useController = operation_behavior_controller_json.get("use_gradient_house", False),
            strategyBehaviorController = operation_behavior_controller_json.get("strategy_gradient_house", ""),
            outboundService = operation_behavior_controller_json.get("outbound_service", "")
        )
        # input
        input = operator_json.get("input", [])
        if input == "":
            input = []
        # model
        model_json = operator_json.get("model", {})
        model_transfer_type_str = model_json.get("model_transfer_type", "S3")
        model = taskService_pb2.Model(
            useModel = model_json.get("use_model", False),
            modelForTrain = model_json.get("model_for_train", False),
            modelTransferType = eval(f"taskService_pb2.FileTransferType.{model_transfer_type_str}"),
            modelPath = model_json.get("model_path", ""),
            modelUpdateStyle = model_json.get("model_update_style", "")
        )
        # logical_simulation_operator_info
        logical_simulation_operator_info_json = operator_json.get("logical_simulation", {})
        operator_transfer_type_str = logical_simulation_operator_info_json.get("operator_transfer_type", "S3")
        logical_simulation_operator_info = taskService_pb2.OperatorSimulationInfo(
            operatorTransferType = eval(f"taskService_pb2.FileTransferType.{operator_transfer_type_str}"),
            operatorCodePath = logical_simulation_operator_info_json.get("operator_code_path", ""),
            operatorEntryFile = logical_simulation_operator_info_json.get("operator_entry_file", ""),
            operatorParams = logical_simulation_operator_info_json.get("operator_params", "")
        )
        # device_simulation_operator_info
        device_simulation_operator_info_json = operator_json.get("device_simulation", {})
        operator_transfer_type_str = device_simulation_operator_info_json.get("operator_transfer_type", "S3")
        device_simulation_operator_info = taskService_pb2.OperatorSimulationInfo(
            operatorTransferType = eval(f"taskService_pb2.FileTransferType.{operator_transfer_type_str}"),
            operatorCodePath = device_simulation_operator_info_json.get("operator_code_path", ""),
            operatorEntryFile = device_simulation_operator_info_json.get("operator_entry_file", ""),
            operatorParams = device_simulation_operator_info_json.get("operator_params", "")
        )
        operator = taskService_pb2.Operator(
            name = operator_json.get("name", ""),
            operationBehaviorController = operation_behavior_controller,
            input = input,
            useData = operator_json.get("use_data", False),
            model = model,
            logicalSimulationOperatorInfo = logical_simulation_operator_info,
            deviceSimulationOperatorInfo = device_simulation_operator_info
        )
        operator_list.append(operator)
    operatorflow = taskService_pb2.OperatorFlow(
        flowSetting = flow_setting,
        operator = operator_list
    )

    # logical_simulation
    logical_simulation_json = jsondata.get("logical_simulation", {})
    computation_unit_json = logical_simulation_json.get("computation_unit", {})
    resource_request_list_json = logical_simulation_json.get("resource_request", {})
    # logical_simulation - computation_unit
    setting_list_json = computation_unit_json.get("setting", [])
    unit_setting_list = []
    for setting_json in setting_list_json:
        unit_setting = taskService_pb2.UnitSetting(
            numCpus = setting_json.get("num_cpus", 0)
        )
        unit_setting_list.append(unit_setting)
    computation_unit = taskService_pb2.ComputationUnit(
        devicesUnit = computation_unit_json.get("devices", []),
        unitSetting = unit_setting_list
    )
    # logical_simulation - resource_request
    resource_request_logical_simulation_list = []
    for resource_request_json in resource_request_list_json:
        resource_request_logical_simulation = taskService_pb2.ResourceRequest(
            dataNameResourceRequest = resource_request_json.get("name", ""),
            deviceResourceRequest = resource_request_json.get("devices", []),
            numResourceRequest = resource_request_json.get("num_request", []),
        )
        resource_request_logical_simulation_list.append(resource_request_logical_simulation)
    logical_simulation = taskService_pb2.LogicalSimulation(
        computationUnit = computation_unit,
        resourceRequestLogicalSimulation = resource_request_logical_simulation_list
    )

    # device_simulation
    device_simulation_json = jsondata.get("device_simulation", {})
    resource_request_list_json = device_simulation_json.get("resource_request", {})
    resource_request_device_simulation_list = []
    for resource_request_json in resource_request_list_json:
        resource_request_device_simulation = taskService_pb2.ResourceRequest(
            dataNameResourceRequest = resource_request_json.get("name", ""),
            deviceResourceRequest = resource_request_json.get("devices", []),
            numResourceRequest = resource_request_json.get("num_request", []),
        )
        resource_request_device_simulation_list.append(resource_request_device_simulation)
    device_simulation = taskService_pb2.DeviceSimulation(
        resourceRequestDeviceSimulation = resource_request_device_simulation_list
    )
    taskconfig = taskService_pb2.TaskConfig(
        userID = jsondata.get("user_id", ""),
        taskID = taskService_pb2.TaskID(taskID=jsondata.get("task_id", "")),
        target = target,
        operatorFlow = operatorflow,
        logicalSimulation = logical_simulation,
        deviceSimulation = device_simulation
    )
    return taskconfig

def taskconfig2json(taskconfig):
    jsondata = dict()

    # target
    target_config = taskconfig.target
    target_data_list = []
    for target_data_config in target_config.targetData:
        total_simulation_config = target_data_config.totalSimulation
        total_simulation = {
            "devices": list(total_simulation_config.deviceTotalSimulation),
            "nums": list(total_simulation_config.numTotalSimulation),
            "dynamic_nums": list(total_simulation_config.dynamicNumTotalSimulation)
        }
        allocation_config = target_data_config.allocation
        running_response_config = allocation_config.runningResponse
        running_response = {
            "devices": list(running_response_config.deviceRunningResponse),
            "nums": list(running_response_config.numRunningResponse)
        }
        allocation = {
            "optimization": allocation_config.optimization,
            "logical_simulation": list(allocation_config.allocationLogicalSimulation),
            "device_simulation": list(allocation_config.allocationDeviceSimulation),
            "running_response": running_response
        }
        target_data = {
            "name": target_data_config.dataName,
            "data_path": target_data_config.dataPath,
            "data_split_type": target_data_config.dataSplitType,
            "data_transfer_type": taskService_pb2.FileTransferType.Name(target_data_config.dataTransferType),
            "task_type": target_data_config.taskType,
            "total_simulation": total_simulation,
            "allocation": allocation,
        }
        target_data_list.append(target_data)
    target = {
        "data": target_data_list,
        "priority": target_config.priority
    }

    # operatorflow
    operatorflow_config = taskconfig.operatorFlow
    flow_setting_config = operatorflow_config.flowSetting
    operator_list_config = operatorflow_config.operator
    start_condition = {
        "logical_simulation": {
            "strategy": flow_setting_config.startCondition.logicalSimulationStrategy.strategyCondition,
            "wait_interval": flow_setting_config.startCondition.logicalSimulationStrategy.waitInterval,
            "total_timeout": flow_setting_config.startCondition.logicalSimulationStrategy.totalTimeout,
        },
        "device_simulation": {
            "strategy": flow_setting_config.startCondition.deviceSimulationStrategy.strategyCondition,
            "wait_interval": flow_setting_config.startCondition.deviceSimulationStrategy.waitInterval,
            "total_timeout": flow_setting_config.startCondition.deviceSimulationStrategy.totalTimeout,
        }
    }
    stop_condition = {
        "logical_simulation": {
            "strategy": flow_setting_config.stopCondition.logicalSimulationStrategy.strategyCondition,
            "wait_interval": flow_setting_config.stopCondition.logicalSimulationStrategy.waitInterval,
            "total_timeout": flow_setting_config.stopCondition.logicalSimulationStrategy.totalTimeout,
        },
        "device_simulation": {
            "strategy": flow_setting_config.stopCondition.deviceSimulationStrategy.strategyCondition,
            "wait_interval": flow_setting_config.stopCondition.deviceSimulationStrategy.waitInterval,
            "total_timeout": flow_setting_config.stopCondition.deviceSimulationStrategy.totalTimeout,
        }
    }
    flow_setting = {
        "round": flow_setting_config.round,
        "start": start_condition,
        "stop": stop_condition
    }
    operator_list = []
    for operator_config in operator_list_config:
        operation_behavior_controller_config = operator_config.operationBehaviorController
        operation_behavior_controller = {
            "use_gradient_house": operation_behavior_controller_config.useController,
            "strategy_gradient_house": operation_behavior_controller_config.strategyBehaviorController,
            "outbound_service": operation_behavior_controller_config.outboundService
        }
        model_config = operator_config.model
        model = {
            "use_model": model_config.useModel,
            "model_for_train": model_config.modelForTrain,
            "model_transfer_type": taskService_pb2.FileTransferType.Name(model_config.modelTransferType),
            "model_path": model_config.modelPath,
            "model_update_style": model_config.modelUpdateStyle
        }
        operator_logical_simulation_config = operator_config.logicalSimulationOperatorInfo
        operator_logical_simulation = {
            "operator_transfer_type": taskService_pb2.FileTransferType.Name(
                operator_logical_simulation_config.operatorTransferType),
            "operator_code_path": operator_logical_simulation_config.operatorCodePath,
            "operator_entry_file": operator_logical_simulation_config.operatorEntryFile,
            "operator_params": operator_logical_simulation_config.operatorParams
        }
        operator_device_simulation_config = operator_config.deviceSimulationOperatorInfo
        operator_device_simulation = {
            "operator_transfer_type": taskService_pb2.FileTransferType.Name(
                operator_device_simulation_config.operatorTransferType),
            "operator_code_path": operator_device_simulation_config.operatorCodePath,
            "operator_entry_file": operator_device_simulation_config.operatorEntryFile,
            "operator_params": operator_device_simulation_config.operatorParams
        }
        operator = {
            "name": operator_config.name,
            "operation_behavior_controller": operation_behavior_controller,
            "input": list(operator_config.input),
            "use_data": operator_config.useData,
            "model": model,
            "logical_simulation": operator_logical_simulation,
            "device_simulation": operator_device_simulation
        }
        operator_list.append(operator)
    operatorflow = {
        "flow_setting": flow_setting,
        "operators": operator_list
    }

    # logical_simulation
    logical_simulation_config = taskconfig.logicalSimulation
    computation_unit_config = logical_simulation_config.computationUnit
    computation_unit_setting_list = []
    for computation_unit_setting_config in computation_unit_config.unitSetting:
        computation_unit_setting = {
            "num_cpus": computation_unit_setting_config.numCpus
        }
        computation_unit_setting_list.append(computation_unit_setting)
    computation_unit = {
        "devices": list(computation_unit_config.devicesUnit),
        "setting": computation_unit_setting_list
    }
    resource_request_list = []
    for resource_request_config in logical_simulation_config.resourceRequestLogicalSimulation:
        resource_request = {
            "name": resource_request_config.dataNameResourceRequest,
            "devices": list(resource_request_config.deviceResourceRequest),
            "num_request": list(resource_request_config.numResourceRequest)
        }
        resource_request_list.append(resource_request)
    logical_simulation = {
        "computation_unit": computation_unit,
        "resource_request": resource_request_list
    }

    # device_simulation
    device_simulation_config = taskconfig.deviceSimulation
    resource_request_list = []
    for resource_request_config in device_simulation_config.resourceRequestDeviceSimulation:
        resource_request = {
            "name": resource_request_config.dataNameResourceRequest,
            "devices": list(resource_request_config.deviceResourceRequest),
            "num_request": list(resource_request_config.numResourceRequest)
        }
        resource_request_list.append(resource_request)
    device_simulation = {
        "resource_request": resource_request_list
    }

    jsondata.update({
        "user_id": taskconfig.userID,
        "task_id": taskconfig.taskID.taskID,
        "target": target,
        "operatorflow": operatorflow,
        "logical_simulation": logical_simulation,
        "device_simulation" : device_simulation
    })
    return jsondata

def get_current_time(mode="UTC"):
    current_time = datetime.datetime.now()
    utc_timezone = pytz.timezone(mode)
    current_time_utc = current_time.astimezone(utc_timezone).strftime("%Y-%m-%d %H:%M:%S")
    return current_time_utc

def get_time_difference(start_time:str, end_time:str):
    time_difference = date_parse(end_time) - date_parse(start_time)
    time_difference = time_difference.total_seconds() #换算成秒
    return time_difference

def write_to_db(task_repo, task_id, item, value,
                logger, system_name, module_name, func_name):
    write_flag = task_repo.set_item_value(task_id=task_id, item=item, value=value)
    if not write_flag:
        logger.error(task_id = task_id,
                     system_name = system_name,
                     module_name = module_name,
                     message=f"[{func_name}]: Failed to set"
                             f" {item}={value} of task_id={task_id}"
                             f" in task_repo.")

def check_task_use_deviceflow(taskconfig):
    use_deviceflow = False
    operatorflow_config = taskconfig.operatorFlow
    operator_list_config = operatorflow_config.operator
    for operator_config in operator_list_config:
        operation_behavior_controller_config = operator_config.operationBehaviorController
        if operation_behavior_controller_config.useController == True:
            use_deviceflow = True
            break
    return use_deviceflow

def unregister_task_in_deviceflow(task_id):
    with open(DEVICEFLOW_CONFIG, 'r') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
        grpc_service_address = config["grpc_service_address"]
    with grpc.insecure_channel(grpc_service_address) as channel:
        deviceflow_stub = deviceflow_pb2_grpc.TaskOperatorOrientedDeviceFlowStub(channel)
        # 删除注册信息
        response = deviceflow_stub.UnRegisterTask(
            deviceflow_pb2.UnRegisterRequest(
                task_id=task_id
            )
        )
        if response.is_success:
            return True
        else:
            return False

def check_deviceflow_dispatch_finished(task_id):
    with open(DEVICEFLOW_CONFIG, 'r') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
        grpc_service_address = config["grpc_service_address"]
    with grpc.insecure_channel(grpc_service_address) as channel:
        deviceflow_stub = deviceflow_pb2_grpc.TaskOperatorOrientedDeviceFlowStub(channel)
        # 查询是否发送完毕
        response = deviceflow_stub.CheckDeviceflowDispatchFinished(
            deviceflow_pb2.CheckDeviceflowDispatchRequest(
                task_id=task_id
            )
        )
        if response.is_success:
            return True
        else:
            return False