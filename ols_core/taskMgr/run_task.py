import sys
import json
import argparse

import ray
from ray.util import remove_placement_group
from ray.util.actor_pool import ActorPool
from ray.util.placement_group import placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

# 路径参数
PYTHONPATH = "/home/code/olearning-simulator/python"
RAY_LOCAL_DATA_PATH = "/home/ray/tempdata"

# 设置PYTHONPATH
sys.path.append(PYTHONPATH)

# extra import
from ols.taskMgr.utils.utils_run_task import Params, GradientHouse, Actor
from ols.taskMgr.utils.operator_factory import OperatorFactory
from ols.taskMgr.utils.operatorflow_factory import OperatorFlowFactory

class RayRunner:
    def __init__(self):
        self.params = None
        self.operator = []
        self.operatorflow = None
        self.logger = None
        self.get_params()
        self.logger = self.params.logger

    def get_params(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--task", type=str, help="get task params in json type")
        args = parser.parse_args()
        params = json.loads(args.task)
        params.update({
            "python_path": PYTHONPATH,
            "ray_local_data_path": RAY_LOCAL_DATA_PATH
        })
        self.params = Params(params=params)

    def initiate(self):
        # 获取梯度中间站
        self.gradient_house = GradientHouse(
            use = self.params.use_gradient_house,
            strategy = self.params.gradient_house_strategy,
            task_id = self.params.task_id,
            config_path = self.params.GRADIENT_HOUSE_CONFIG,
            logger = self.logger
        )
        # 获取 Operator列表、OperatorFlow
        operator_name_list = []
        self.operator = []
        for operator_params in self.params.operators:
            operator_name = operator_params.get("name", "")
            self.operator.append(
                OperatorFactory.create_operator(
                    operator_name = operator_params.get("name", ""),
                    params = operator_params
                )
            )
            operator_name_list.append(operator_name)
        self.params.operator_name_list = operator_name_list
        # 获得有效的算子流
        operatorflow_name = "->".join(operator_name_list)
        self.operatorflow = OperatorFlowFactory.create_operator_flow(
            operatorflow_name=operatorflow_name,
            params = self.params
        )

    def update_operatorflow_params(self, round_idx):
        for operator in self.operator:
            self.params.OPERATOR_RESULTS_DICT.update({
                operator.name: f"{self.params.results_upload_path}/round{round_idx}/{operator.name}"
            })

    def update_operator_params(self, operator, round_idx):
        # get round_idx
        operator.round_idx = round_idx
        # get phones for actors
        cpu_num, phones_num = operator.cpu, operator.number_phones
        cpu_num = max(int(cpu_num) + 1 if cpu_num > int(cpu_num) else int(cpu_num), 1)
        phones_for_actors = [phones_num // cpu_num] * cpu_num
        for i in range(phones_num % cpu_num):
            phones_for_actors[i] = phones_for_actors[i] + 1
        operator.phones_for_actors = phones_for_actors
        # get operatorflow_name
        operator.operator_name_list = self.params.operator_name_list
        # get params
        operator.params = self.params
        # get PYTHONPATH
        operator.PYTHONPATH = self.params.PYTHONPATH
        return operator

    def get_actor_pool(self, cpu_num=1):
        cpu_num = max(int(cpu_num) + 1 if cpu_num > int(cpu_num) else int(cpu_num), 1)
        worker_config = [{"CPU": 1}] * cpu_num
        pg = placement_group(worker_config, strategy="SPREAD")
        # ready, unready = ray.wait([pg.ready()], timeout=10)
        ready, unready = ray.wait([pg.ready()])
        actorsList = [Actor.options(scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg, placement_group_bundle_index=i)).remote() for i in range(cpu_num)]
        pool = ActorPool(actorsList)
        return pool, pg

    def actor_run(self, operator, run_params):
        pool, pg = self.get_actor_pool(cpu_num=operator.cpu)
        status_list_remote = []
        pool.map_unordered(lambda a, v: status_list_remote.append(a.run.remote(v)), run_params)
        status_list = ray.get(status_list_remote)
        remove_placement_group(pg)
        return status_list

    def analyze_results(self, operator, status_list):
        task_id = self.params.task_id
        operator_name = operator.name
        task_repo = self.params.task_repo
        logger = self.logger
        # 统计所有成功和失败的手机数
        success_num, failed_num = 0, 0
        for actor_status in status_list:
            success_num += actor_status.get("success", 0)
            failed_num += actor_status.get("failed", 0)
        logger.info(task_id=task_id, system_name="Ray Runner", module_name="run_task",
                    message=f"[run] round={operator.round_idx}, operator= {operator_name}, success_num = {success_num}, failed_num={failed_num}")
        if not task_repo.set_item_value(task_id=task_id, item="logical_round", value=(operator.round_idx + 1)):
            logger.error(task_id=task_id, system_name="Ray Runner", module_name="run_task",
                         message=f"[run] set logical_round={operator.round_idx} of task_id={task_id} failed!")
        if not task_repo.set_item_value(task_id=task_id, item="logical_operator", value=operator_name):
            logger.error(task_id=task_id, system_name="Ray Runner", module_name="run_task",
                         message=f"[run] set logical_operator={operator_name} of task_id={task_id} failed!")
        if not task_repo.set_item_value(task_id=task_id, item="logical_success_num", value=success_num):
            logger.error(task_id=task_id, system_name="Ray Runner", module_name="run_task",
                         message=f"[run] set logical_success_num={success_num} of task_id={task_id} failed!")
        if not task_repo.set_item_value(task_id=task_id, item="logical_failed_num", value=failed_num):
            logger.error(task_id=task_id, system_name="Ray Runner", module_name="run_task",
                         message=f"[run] set logical_failed_num={failed_num} of task_id={task_id} failed!")

    def run(self):
        self.initiate()
        for round_idx in range(self.params.round):
            ### 更新参数 ###
            self.params.current_round = round_idx
            self.update_operatorflow_params(round_idx=round_idx)
            ### 执行 ###
            # 算子流启动
            operatorflow_start_status = self.operatorflow.start()
            if not operatorflow_start_status:
                self.logger.error(task_id=self.params.task_id, system_name="Ray Runner", module_name="run_task",
                                 message=f"[run]: round {round_idx}, operatorflow start failed!")
                raise Exception(f"[run]: round {round_idx}, operatorflow start failed!")

            # 使用梯度中间站
            gradient_house_start_status = self.gradient_house.start()
            if not gradient_house_start_status:
                self.logger.error(task_id=self.params.task_id, system_name="Ray Runner", module_name="run_task",
                                 message=f"[run]: round {round_idx}, gradient_house start failed!")
                raise Exception(f"[run]: round {round_idx}, gradient_house start failed!")

            # 算子逐个执行
            for operator in self.operator:
                self.logger.info(task_id=self.params.task_id, system_name="Ray Runner", module_name="run_task",
                            message=f"[run]: round {round_idx}, operator = {operator.name} begin!")
                operator = self.update_operator_params(operator, round_idx) # 更新参数
                run_params = operator.construct_run_params()
                status_list = self.actor_run(operator=operator, run_params=run_params) # 获取各Actor的结果
                self.analyze_results(operator, status_list) # 分析结果
                self.logger.info(task_id=self.params.task_id, system_name="Ray Runner", module_name="run_task",
                            message=f"[run]: round {round_idx}, operator = {operator.name} end!")

            # 停止梯度中间站
            gradient_house_stop_status = self.gradient_house.stop()
            if not gradient_house_stop_status:
                self.logger.error(task_id=self.params.task_id, system_name="Ray Runner", module_name="run_task",
                                 message=f"[run]: round {round_idx}, gradient_house stop failed!")
                raise Exception(f"[run]: round {round_idx}, gradient_house stop failed!")

            # 算子流结束判断
            operatorflow_stop_status = self.operatorflow.stop()
            if not operatorflow_stop_status:
                self.logger.error(task_id=self.params.task_id, system_name="Ray Runner", module_name="run_task",
                                 message=f"[run]: round {round_idx}, operatorflow stop failed!")
                if round_idx < self.params.round - 1:
                    raise Exception(f"[run]: round {round_idx}, operatorflow stop failed!")
                else: #如果是最后一轮，实际上已经完成了，所以可以不用等
                    break

if __name__ == '__main__':
    ray_runner = RayRunner()
    ray_runner.run()
