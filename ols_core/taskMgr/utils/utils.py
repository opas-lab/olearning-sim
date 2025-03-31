import os
import json
import yaml
import pytz
import shutil
import zipfile
import logging
import datetime
import sqlalchemy
import pandas as pd
from ols.simu_log import Logger
from ols.proto import taskService_pb2
from sqlalchemy import MetaData, Table, update, select, and_

# filepath
FILEPATH = os.path.abspath(os.path.dirname(__file__))   # task_manager.py文件所在的绝对路径
OLSPATH = os.path.abspath(os.path.join(FILEPATH, '../..')) # ols所在文件夹的绝对路径
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
        except Exception as e:
            return []

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
            assert isinstance(request.gradientHouse.useGradientHouse, bool), "GradientHouse.useGradientHouse should be bool"
            assert isinstance(request.gradientHouse.strategyGradientHouse, str), "GradientHouse.strategyGradientHouse should be str"
            # logical
            assert isinstance(request.logicalSimulation.taskID.taskID, str), "logicalSimulation.taskID should be str"
            assert isinstance(request.logicalSimulation.simuDataPath, str), "logicalSimulation.simuDataPath should be str"
            assert isinstance(request.logicalSimulation.fileTransferType, int), "logicalSimulation.fileTransferType should be int"
            assert isinstance(request.logicalSimulation.datasetSplitType, bool), "logicalSimulation.datasetSplitType should be bool"
            assert isinstance(request.logicalSimulation.round, int), "logicalSimulation.round should be int"
            assert isinstance(request.logicalSimulation.gradientHouse.useGradientHouse, bool), "logicalSimulation.GradientHouse.useGradientHouse should be bool"
            assert isinstance(request.logicalSimulation.gradientHouse.strategyGradientHouse, str), "logicalSimulation.GradientHouse.strategyGradientHouse should be str"
            for operator_idx, operator in enumerate(request.logicalSimulation.operators):
                if isinstance(operator.name, str):
                    operator_name = operator.name
                else:
                    raise AssertionError(f"The name of No.{operator_idx} operator in operators should be str")
                assert isinstance(operator.operatorCodePath, str), f"operator={operator_name} operatorCodePath should be str"
                assert isinstance(operator.operatorEntryFile, str), f"operator={operator_name} operatorEntryFile should be str"
                assert isinstance(operator.simuParams.numberPhones, int), f"operator={operator_name} numberPhones should be int"
                assert isinstance(operator.simuParams.dynamicPhones, int), f"operator={operator_name} dynamicPhones should be int"
                assert isinstance(operator.simuParams.resourceRequest.cpu, float), f"operator={operator_name} resourceRequest.cpu should be float"
                assert isinstance(operator.simuParams.resourceRequest.mem, float), f"operator={operator_name} resourceRequest.mem should be float"
                assert isinstance(operator.opParams.paramsJson, str), f"operator={operator_name} opParams.paramsJson should be str"
            # device
            assert isinstance(request.deviceSimulation.apkPath, str), "deviceSimulation.apkPath should be str"
            assert isinstance(request.deviceSimulation.modelPath, str), "deviceSimulation.modelPath should be str"
            assert isinstance(request.deviceSimulation.dataPath, str), "deviceSimulation.dataPath should be str"
            assert isinstance(request.deviceSimulation.taskID, str), "deviceSimulation.taskID should be str"
            for requirement in request.deviceSimulation.deviceRequirement:
                assert isinstance(requirement.phoneType, str), "deviceSimulation.deviceRequirement.phoneType should be str"
                assert isinstance(requirement.num, int), "deviceSimulation.deviceRequirement.num should be int"
            assert isinstance(request.deviceSimulation.gradientHouse.useGradientHouse, bool), "deviceSimulation.GradientHouse.useGradientHouse should be bool"
            assert isinstance(request.deviceSimulation.gradientHouse.strategyGradientHouse, str), "deviceSimulation.GradientHouse.strategyGradientHouse should be str"
            assert isinstance(request.deviceSimulation.operator, int), "deviceSimulation.operator should be int"
            assert isinstance(request.deviceSimulation.mode, int), "deviceSimulation.mode should be int"
            assert isinstance(request.deviceSimulation.simulateNum, int), "deviceSimulation.simulateNum should be int"
            assert isinstance(request.deviceSimulation.dynamicNum, int), "deviceSimulation.dynamicNum should be int"
            assert isinstance(request.deviceSimulation.fileAddress, int), "deviceSimulation.fileAddress should be int"
            assert isinstance(request.deviceSimulation.paramsJson, str), "deviceSimulation.paramsJson should be str"
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
            # logical
            if request.logicalSimulation.operators:
                assert request.userID != "", "userID should not be empty"
                assert self.is_in_ascii(request.userID), f"userID={request.userID} contains illegal characters. It should consist of English characters, symbols, and numbers."
                assert request.taskID.taskID != "", "taskID should not be empty"
                assert self.is_in_ascii(request.taskID.taskID), f"taskID={request.taskID.taskID} contains illegal characters. It should consist of English characters, symbols, and numbers."
                assert self.is_in_ascii(request.gradientHouse.strategyGradientHouse), f"GradientHouse.strategyGradientHouse={request.gradientHouse.strategyGradientHouse} contains illegal characters. It should consist of English characters, symbols, and numbers."
                assert request.logicalSimulation.taskID.taskID != "", "logicalSimulation.taskID should not be empty"
                assert self.is_in_ascii(request.logicalSimulation.taskID.taskID), f"logicalSimulation.taskID={request.logicalSimulation.taskID.taskID} contains illegal characters. It should consist of English characters, symbols, and numbers."
                if request.logicalSimulation.simuDataPath: #不一定所有任务都需要数据路径，如果有，则需要判断
                    assert self.is_file_ext(s=request.logicalSimulation.simuDataPath, ext=".zip"), f"logicalSimulation.simuDataPath={request.logicalSimulation.simuDataPath} should be a file path with a .zip extension."
                try:
                    taskService_pb2.FileTransferType.Name(request.logicalSimulation.fileTransferType)
                except:
                    raise AssertionError(f"The value of logicalSimulation.fileTransferType={request.logicalSimulation.fileTransferType} is not valid.")
                assert request.logicalSimulation.round > 0, f"logicalSimulation.round={request.logicalSimulation.round} should be larger than 0"
                assert self.is_in_ascii(request.logicalSimulation.gradientHouse.strategyGradientHouse), f"logicalSimulation.gradientHouse.strategyGradientHouse={request.logicalSimulation.gradientHouse.strategyGradientHouse} contains illegal characters. It should consist of English characters, symbols, and numbers."
                for operator_idx, operator in enumerate(request.logicalSimulation.operators):
                    assert operator.name != "", f"The name of No.{operator_idx} operator in operators should not be empty"
                    assert self.is_in_ascii(operator.name), f"The name of No.{operator_idx} operator in operators = {operator.name} contains illegal characters. It should consist of English characters, symbols, and numbers."
                    if not (os.path.isdir(os.path.abspath(operator.operatorCodePath)) or self.is_file_ext(s=operator.operatorCodePath, ext=".zip")):
                        raise AssertionError(f"operator.operatorCodePath of {operator.name} = {operator.operatorCodePath} should be an existing directory or a file path with a .zip extension.")
                    assert self.is_file_ext(s=operator.operatorEntryFile, ext=".py"), f"operator.operatorEntryFile of {operator.name} = {operator.operatorEntryFile} should be a file path with a .py extension."
                    assert operator.simuParams.numberPhones > 0, f"operator={operator.name} numberPhones = {operator.simuParams.numberPhones} should be larger than 0"
                    assert operator.simuParams.dynamicPhones >= 0, f"operator={operator.name} dynamicPhones = {operator.simuParams.dynamicPhones} should not be less than 0"
                    assert operator.simuParams.resourceRequest.cpu > 0, f"operator={operator.name} resourceRequest.cpu = {operator.simuParams.resourceRequest.cpu} should be larger than 0"
                    assert operator.simuParams.resourceRequest.mem > 0, f"operator={operator.name} resourceRequest.mem = {operator.simuParams.resourceRequest.mem} should be larger than 0"
                    if operator.opParams.paramsJson: #如果具有算子参数，需要判断是否为json形式
                        try:
                            json.loads(operator.opParams.paramsJson)
                        except:
                            raise AssertionError(f"operator={operator.name} opParams.paramsJson should be a json string")
            # device
            if request.deviceSimulation.deviceRequirement:
                assert self.is_in_ascii(request.deviceSimulation.apkPath), f"deviceSimulation.apkPath={request.deviceSimulation.apkPath} contains illegal characters. It should consist of English characters, symbols, and numbers."
                assert self.is_in_ascii(request.deviceSimulation.modelPath), f"deviceSimulation.modelPath={request.deviceSimulation.modelPath} contains illegal characters. It should consist of English characters, symbols, and numbers."
                if request.deviceSimulation.dataPath:  # 不一定所有任务都需要数据路径，如果有，则需要判断
                    assert self.is_file_ext(s=request.deviceSimulation.dataPath, ext=".zip"), f"deviceSimulation.dataPath={request.deviceSimulation.dataPath} should be a file path with a .zip extension."
                assert request.deviceSimulation.taskID != "", "deviceSimulation.taskID should not be empty"
                assert self.is_in_ascii(request.deviceSimulation.taskID), f"deviceSimulation.taskID={request.deviceSimulation.taskID} contains illegal characters. It should consist of English characters, symbols, and numbers."
                for requirement in request.deviceSimulation.deviceRequirement:
                    assert self.is_in_ascii(requirement.phoneType), f"requirement.phoneType={requirement.phoneType} contains illegal characters. It should consist of English characters, symbols, and numbers."
                    assert requirement.num>0, f"deviceSimulation.deviceRequirement.num={requirement.num} should be larger than 0"
                assert self.is_in_ascii(request.deviceSimulation.gradientHouse.strategyGradientHouse), f"deviceSimulation.gradientHouse.strategyGradientHouse={request.deviceSimulation.gradientHouse.strategyGradientHouse} contains illegal characters. It should consist of English characters, symbols, and numbers."
                try:
                    taskService_pb2.OperatorType.Name(request.deviceSimulation.operator)
                except:
                    raise AssertionError(f"The value of deviceSimulation.operator={request.deviceSimulation.operator} is not valid.")
                try:
                    taskService_pb2.ModeType.Name(request.deviceSimulation.mode)
                except:
                    raise AssertionError(f"The value of deviceSimulation.mode={request.deviceSimulation.mode} is not valid.")
                assert request.deviceSimulation.simulateNum > 0, f"deviceSimulation.simulateNum={request.deviceSimulation.simulateNum} should be larger than 0"
                assert request.deviceSimulation.dynamicNum >= 0, f"deviceSimulation.dynamicNum={request.deviceSimulation.dynamicNum} should not be less than 0"
                try:
                    taskService_pb2.FileTransferType.Name(request.deviceSimulation.fileAddress)
                except:
                    raise AssertionError(f"The value of deviceSimulation.fileAddress={request.deviceSimulation.fileAddress} is not valid.")
                if request.deviceSimulation.paramsJson:  # 如果具有算子参数，需要判断是否为json形式
                    try:
                        json.loads(request.deviceSimulation.paramsJson)
                    except:
                        raise AssertionError("deviceSimulation.paramsJson should be a json string")
            return True
        except Exception as e:
            logger.error(task_id=request.taskID.taskID, system_name="TaskMgr", module_name="task_manager",
                         message=f"[validate_task_parameters] ValueError: {e}")
            return False

    def validate_relationship(self, request):
        try:
            # 使用梯度中间站时，策略不能为空
            if request.gradientHouse.useGradientHouse:
                assert request.gradientHouse.strategyGradientHouse != "", f"GradientHouse.strategyGradientHouse should not be empty when useGradientHouse is True"

            # logical
            if request.logicalSimulation.operators:
                # logicalSimulation里面的task_id需要和任务的task_id相同
                assert request.taskID.taskID == request.logicalSimulation.taskID.taskID, f"request.taskID.taskID({request.taskID.taskID}) is not equal to request.logicalSimulation.taskID.taskID({request.logicalSimulation.taskID.taskID})"
                # 使用梯度中间站时，策略不能为空
                if request.logicalSimulation.gradientHouse.useGradientHouse:
                    assert request.logicalSimulation.gradientHouse.strategyGradientHouse != "", f"logicalSimulation.GradientHouse.strategyGradientHouse should not be empty when useGradientHouse is True"
                for operator in request.logicalSimulation.operators:
                    # operator.operatorCodePath的dir最后一级文件夹名，或zip包前的文件名，需要和算子名称相同
                    if self.is_file_ext(s=operator.operatorCodePath, ext=".zip"):
                        filename, _ = os.path.splitext(operator.operatorCodePath)
                    else:
                        filename = os.path.abspath(operator.operatorCodePath)
                    if os.path.basename(filename) != operator.name:
                        raise AssertionError(f"The last-level directory name of operator.operatorCodePath, or the filename before the zip extension, {filename}, needs to be the same as the {operator.name}.")
                    # 如果operator.operatorCodePath是dir，只允许FileTransferType为FILE
                    if os.path.isdir(os.path.abspath(operator.operatorCodePath)):
                        assert taskService_pb2.FileTransferType.Name(request.logicalSimulation.fileTransferType) == "FILE", f"operator={operator.name}, if operator.operatorCodePath is a dir, only FILE is allowed for FileTransferType"
                    # number_phones应该大于dynamic_phones
                    assert operator.simuParams.numberPhones > operator.simuParams.dynamicPhones, f"operator={operator.name} numberPhones({operator.simuParams.numberPhones}) should be larger than dynamicPhones ({operator.simuParams.dynamicPhones})"
                    # number_phones应该大于等于所需要申请的CPU数
                    assert operator.simuParams.numberPhones >= operator.simuParams.resourceRequest.cpu, f"operator={operator.name} numberPhones({operator.simuParams.numberPhones}) should be larger than resourceRequest.cpu ({operator.simuParams.resourceRequest.cpu})"
            # device
            if request.deviceSimulation.deviceRequirement:
                # deviceSimulation里面的task_id需要和任务的task_id相同
                assert request.taskID.taskID == request.deviceSimulation.taskID, f"request.taskID.taskID({request.taskID.taskID}) is not equal to request.deviceSimulation.taskID({request.deviceSimulation.taskID})"
                # 使用梯度中间站时，策略不能为空
                if request.deviceSimulation.gradientHouse.useGradientHouse:
                    assert request.deviceSimulation.gradientHouse.strategyGradientHouse != "", f"deviceSimulation.GradientHouse.strategyGradientHouse should not be empty when useGradientHouse is True"
                # simulateNum应该大于dynamicNum
                assert request.deviceSimulation.simulateNum > request.deviceSimulation.dynamicNum, f"deviceSimulation.simulateNum={request.deviceSimulation.simulateNum} should be larger than deviceSimulation.dynamicNum={request.deviceSimulation.dynamicNum}"
                # 如果是SINGLE模式，请求的手机总数应该等于simulateNum
                if taskService_pb2.ModeType.Name(request.deviceSimulation.mode)=="SINGLE":
                    sum_phone_num = 0
                    for requirement in request.deviceSimulation.deviceRequirement:
                        sum_phone_num += requirement.num
                    assert sum_phone_num == request.deviceSimulation.simulateNum, f"deviceSimulation.simulateNum={request.deviceSimulation.simulateNum} should be equal to the sum of require phone nums ({sum_phone_num})"
                # 如果是MULTI模式，请求的手机总数小于等于simulateNum
                if taskService_pb2.ModeType.Name(request.deviceSimulation.mode) == "MULTI":
                    sum_phone_num = 0
                    for requirement in request.deviceSimulation.deviceRequirement:
                        sum_phone_num += requirement.num
                    assert sum_phone_num <= request.deviceSimulation.simulateNum, f"The sum of require phone nums ({sum_phone_num}) should be less than or equal to deviceSimulation.simulateNum={request.deviceSimulation.simulateNum}."

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
    # 梯度中间站
    gradient_house_json = jsondata.get("gradient_house", {})
    gradient_house = taskService_pb2.GradientHouse(
        useGradientHouse = gradient_house_json.get("use_gradient_house", False),
        strategyGradientHouse = gradient_house_json.get("strategy_gradient_house", "")
    )
    # 逻辑仿真
    logical_simulation_json = jsondata.get("logical_simulation", {})
    file_transfer_type_str = logical_simulation_json.get("file_transfer_type", "FILE")
    operators_json = logical_simulation_json.get("operators", {})
    operators = list()
    for operator_idx in range(len(operators_json)):
        operator_json = operators_json[operator_idx]
        resourceRequest = taskService_pb2.ResourceRequest(
            cpu = operator_json.get("simulation_params", {}).get("resource_request", {}).get("cpu", 0.0),
            mem = operator_json.get("simulation_params", {}).get("resource_request", {}).get("mem", 0.0)
        )
        simulationParams = taskService_pb2.SimulationParams(
            numberPhones = operator_json.get("simulation_params", {}).get("number_phones", 0),
            dynamicPhones = operator_json.get("simulation_params", {}).get("dynamic_phones", 0),
            resourceRequest = resourceRequest
        )
        opParams = taskService_pb2.OperatorParams(
            paramsJson = json.dumps(operator_json.get("operator_params", {}))
        )
        operator = taskService_pb2.Operator(
            name=operator_json.get("name", ""),
            operatorCodePath = operator_json.get("operator_code_path", ""),
            operatorEntryFile = operator_json.get("operator_entry_file", ""),
            simuParams = simulationParams,
            opParams = opParams
        )
        operators.append(operator)
    logical_simulation = taskService_pb2.LinuxSimuTaskConfig(
        taskID = taskService_pb2.TaskID(taskID=logical_simulation_json.get("task_id", "")),
        simuDataPath = logical_simulation_json.get("simu_data_path", ""),
        fileTransferType = eval(f"taskService_pb2.FileTransferType.{file_transfer_type_str}"),
        datasetSplitType = logical_simulation_json.get("dataset_split_type", False),
        round = logical_simulation_json.get("round", 0),
        gradientHouse = gradient_house,
        operators = operators
    )
    # 真机设备参数
    device_simulation_json = jsondata.get("device_simulation", {})
    device_requirements = list()
    device_requirements_json = device_simulation_json.get("device_requirement", [])
    for device_requirement_idx in range(len(device_requirements_json)):
        device_requirement_json = device_requirements_json[device_requirement_idx]
        phone_type = list(device_requirement_json.keys())[0]
        device_requirement = taskService_pb2.DeviceRequirement(
            phoneType = phone_type,
            num = device_requirement_json.get(phone_type, 0)
        )
        device_requirements.append(device_requirement)
    device_operator = device_simulation_json.get("operator", "ST")
    device_mode = device_simulation_json.get("mode", "SINGLE")
    device_file_address = device_simulation_json.get("file_address", "FILE")
    device_simulation = taskService_pb2.DeviceTaskConfig(
        apkPath = device_simulation_json.get("apk_path", ""),
        modelPath = device_simulation_json.get("model_path", ""),
        dataPath = device_simulation_json.get("data_path", ""),
        taskID = device_simulation_json.get("task_id", ""),
        deviceRequirement = device_requirements,
        gradientHouse = gradient_house,
        operator = eval(f"taskService_pb2.OperatorType.{device_operator}"),
        mode = eval(f"taskService_pb2.ModeType.{device_mode}"),
        simulateNum = device_simulation_json.get("simulate_num", 0),
        dynamicNum = device_simulation_json.get("dynamic_num", 0),
        fileAddress = eval(f"taskService_pb2.FileTransferType.{device_file_address}"),
        paramsJson = json.dumps(device_simulation_json.get("params_json", {}))
    )
    taskconfig = taskService_pb2.TaskConfig(
        userID = jsondata.get("user_id", ""),
        taskID = taskService_pb2.TaskID(taskID=jsondata.get("task_id", "")),
        gradientHouse = gradient_house,
        logicalSimulation = logical_simulation,
        deviceSimulation = device_simulation
    )
    return taskconfig

def taskconfig2json(taskconfig):
    jsondata = dict()
    gradient_house = {
        "use_gradient_house": taskconfig.gradientHouse.useGradientHouse,
        "strategy_gradient_house": taskconfig.gradientHouse.strategyGradientHouse
    }
    operators = []
    for taskconfig_operator in taskconfig.logicalSimulation.operators:
        operator_params = taskconfig_operator.opParams.paramsJson
        if not operator_params:
            operator_params = "{}"
        resource_request = {
            "cpu": taskconfig_operator.simuParams.resourceRequest.cpu,
            "mem": taskconfig_operator.simuParams.resourceRequest.mem
        }
        simulation_params = {
            "number_phones": taskconfig_operator.simuParams.numberPhones,
            "dynamic_phones": taskconfig_operator.simuParams.dynamicPhones,
            "resource_request": resource_request
        }
        operator = {
            "name": taskconfig_operator.name,
            "operator_code_path": taskconfig_operator.operatorCodePath,
            "operator_entry_file": taskconfig_operator.operatorEntryFile,
            "simulation_params": simulation_params,
            "operator_params": json.loads(operator_params)
        }
        operators.append(operator)
    logical_simulation = {
        "task_id": taskconfig.logicalSimulation.taskID.taskID,
        "simu_data_path": taskconfig.logicalSimulation.simuDataPath,
        "file_transfer_type": taskService_pb2.FileTransferType.Name(taskconfig.logicalSimulation.fileTransferType),
        "dataset_split_type": taskconfig.logicalSimulation.datasetSplitType,
        "round": taskconfig.logicalSimulation.round,
        "gradient_house": {
            "use_gradient_house": taskconfig.logicalSimulation.gradientHouse.useGradientHouse,
            "strategy_gradient_house": taskconfig.logicalSimulation.gradientHouse.strategyGradientHouse
        },
        "operators": operators
    }
    device_requirement = []
    for device_simulation_requirement in taskconfig.deviceSimulation.deviceRequirement:
        device_requirement_dict = dict()
        device_requirement_dict.update({
            f"{device_simulation_requirement.phoneType}": device_simulation_requirement.num
        })
        device_requirement.append(device_requirement_dict)
    params_json = taskconfig.deviceSimulation.paramsJson
    if not params_json:
        params_json = "{}"
    device_simulation = {
        "apk_path": taskconfig.deviceSimulation.apkPath,
        "model_path": taskconfig.deviceSimulation.modelPath,
        "data_path": taskconfig.deviceSimulation.dataPath,
        "task_id": taskconfig.deviceSimulation.taskID,
        "device_requirement": device_requirement,
        "gradient_house": {
            "use_gradient_house": taskconfig.deviceSimulation.gradientHouse.useGradientHouse,
            "strategy_gradient_house": taskconfig.deviceSimulation.gradientHouse.strategyGradientHouse
        },
        "operator": taskService_pb2.OperatorType.Name(taskconfig.deviceSimulation.operator),
        "mode": taskService_pb2.ModeType.Name(taskconfig.deviceSimulation.mode),
        "simulate_num": taskconfig.deviceSimulation.simulateNum,
        "dynamic_num": taskconfig.deviceSimulation.dynamicNum,
        "file_address": taskService_pb2.FileTransferType.Name(taskconfig.deviceSimulation.fileAddress),
        "params_json": json.loads(params_json)
    }
    jsondata.update({
        "user_id": taskconfig.userID,
        "task_id": taskconfig.taskID.taskID,
        "gradient_house": gradient_house,
        "logical_simulation": logical_simulation,
        "device_simulation": device_simulation
    })
    return jsondata

def generateRunTaskFile(working_dir=""):
    try:
        currentPath = os.path.abspath(os.path.dirname(__file__))
        parentPath = os.path.abspath(os.path.join(currentPath, '..'))
        save_path = f"{working_dir}/run_task.py"
        shutil.copy2(f"{parentPath}/run_task.py", save_path)
        return True
    except:
        return False

def get_operator_code(file_transfer_type=0, operators=[], working_dir=""):
    for opertor in operators:
        operator_name = opertor.get("name", "")
        operator_code_path = opertor.get("operator_code_path", "")
        file_transfer_type_Name = taskService_pb2.FileTransferType.Name(file_transfer_type)

        # 如果代码在本地，且operator_code_path存在，且是路径dir的形式，则直接复制到对应目录
        if file_transfer_type_Name == "FILE" and os.path.isdir(os.path.abspath(operator_code_path)):
            working_dir_operator = f"{working_dir}/{operator_name}"
            try:
                shutil.copytree(operator_code_path, working_dir_operator)
            except Exception as e:
                logger.error(task_id="", system_name="TaskMgr", module_name="task_runner",
                             message=f"[get_operator_code]: file_transfer_type={file_transfer_type_Name}, operator_code_path={operator_code_path}, copytree failed, because of {e}")
                return False
        else:  # 除了上述情况外，代码文件均为zip包形式
            zip_file_name = os.path.basename(operator_code_path)
            zip_folder_name, _ = os.path.splitext(zip_file_name)
            operator_zip_path = f"{working_dir}/" + zip_file_name

            # 把zip包复制或下载到working_dir
            if file_transfer_type_Name == "FILE":
                try:
                    shutil.copy2(operator_code_path, operator_zip_path)
                except:
                    logger.error(task_id="", system_name="TaskMgr", module_name="task_runner",
                                 message=f"[get_operator_code]: file_transfer_type={file_transfer_type_Name}, operator_code_path={operator_code_path}, copy to working_dir failed")
                    return False

            if file_transfer_type_Name == "HTTP":
                import wget
                import urllib.request
                try:
                    with urllib.request.urlopen(operator_code_path, timeout=10) as response:
                        headers = response.info()
                        content_length = int(response.headers['Content-Length'])
                    if content_length > 0:
                        wget.download(operator_code_path, out=operator_zip_path)
                    else:
                        logger.error(task_id="", system_name="TaskMgr", module_name="task_runner",
                                     message=f"[get_operator_code]: file_transfer_type={file_transfer_type_Name}, operator_code_path = {operator_code_path} is empty!")
                        return False
                except Exception as e:
                    logger.error(task_id="", system_name="TaskMgr", module_name="task_runner",
                                 message=f"[get_operator_code]: file_transfer_type={file_transfer_type_Name}, download {operator_code_path} failed, because of {e}")
                    return False

            if file_transfer_type_Name == "S3":
                from ols.taskMgr.utils.utils_oflserver import downloadS3File
                current_file_path = os.path.abspath(os.path.dirname(__file__))
                ols_path = os.path.abspath(os.path.join(current_file_path, '../..'))
                isDownloadZipFile = downloadS3File(
                    manager_config=ols_path + "/config/manager_config.yaml",
                    local_file=operator_zip_path,
                    taget_file=operator_code_path
                )
                if isDownloadZipFile == False:
                    logger.error(task_id="", system_name="TaskMgr", module_name="task_runner",
                                 message=f"[get_operator_code]: file_transfer_type={file_transfer_type_Name}, download {operator_code_path} failed")
                    return False

            try:
                zip_file = zipfile.ZipFile(operator_zip_path, "r")
                zip_file.extractall(path=working_dir)
                zip_file.close()
                os.remove(operator_zip_path)  # 删除复制的代码zip包文件
            except Exception as e:
                logger.error(task_id="", system_name="TaskMgr", module_name="task_runner",
                             message=f"[get_operator_code]: zip_file.extractall of os.remove failed, because of {e}")
                return False
    return True

def get_current_time(mode="UTC"):
    current_time = datetime.datetime.now()
    utc_timezone = pytz.timezone(mode)
    current_time_utc = current_time.astimezone(utc_timezone).strftime("%Y-%m-%d %H:%M:%S")
    return current_time_utc

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



