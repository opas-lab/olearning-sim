import json
from typing import List, Dict, Any
import time

from datetime import datetime
import pytz

import random
import math
import numpy as np

AREA_CALCULATION_NUM = 100

class Strategy(object):
    def __init__(self):
        pass

    # TODO: 完成有关是否实时转发的判断
    @staticmethod
    def check_real_time_dispatch(strategy: str) -> bool:
        strategy = json.loads(strategy)
        is_real_time_dispatch = strategy.get("real_time_dispatch", {}).get("use_strategy", False)
        return is_real_time_dispatch

    @staticmethod
    def real_time_strategy_analysis(strategy: str) -> (List[int], float):
        strategy = json.loads(strategy)
        real_time_dispatch = strategy.get("real_time_dispatch", {})
        dispatch_amount = real_time_dispatch.get("dispatch_batch_sizes", [])
        drop_probability = real_time_dispatch.get("drop_simulation", {}).get("drop_probability", 0)
        return dispatch_amount, drop_probability

    @staticmethod
    def flow_strategy_analysis(strategy: str, flow_id: str) -> (List[int], List[int], List[List[int]]):
        strategy = json.loads(strategy)
        flow_dispatch = strategy.get("flow_dispatch", {})
        is_flow_strategy_dispatch = flow_dispatch.get("use_strategy", False)
        if not is_flow_strategy_dispatch:
            return [], [], []

        total_dispatch_amount = flow_dispatch.get("total_dispatch_amount", 0)
        if total_dispatch_amount <= 0:
            return [], [], []

        is_specific_timing = flow_dispatch.get("specific_timing", {}).get("use", False)
        is_specific_interval = flow_dispatch.get("specific_interval", {}).get("use", False)

        if is_specific_timing and is_specific_interval:
            return [], [], []
        if is_specific_timing or is_specific_interval:
            strategy = Strategy()
            if is_specific_timing:
                # 特定时间点发送设定量信息
                specific_timing = flow_dispatch.get("specific_timing", {})
                dispatch_timing, dispatch_amount, drop_simulation_list = strategy._specific_timing_analysis(
                    specific_timing = specific_timing,
                    flow_id = flow_id
                )
                return dispatch_timing, dispatch_amount, drop_simulation_list
            else:
                # 特定时间段，按照设定的规则发送信息
                specific_interval = flow_dispatch.get("specific_interval", {})
                dispatch_timing, dispatch_amount, drop_simulation_list = strategy._specific_interval_analysis(
                    total_dispatch_amount = total_dispatch_amount,
                    specific_interval = specific_interval,
                    flow_id = flow_id
                )
                return dispatch_timing, dispatch_amount, drop_simulation_list
        else:
            return [], [], []


    def _specific_timing_analysis(self, specific_timing: Dict[str, Any], flow_id: str):
        time_type = specific_timing.get("time_type", "relative")
        time_zone = specific_timing.get("time_zone", "Asia/Shanghai")

        # 处理absolute类型多轮执行的问题
        if time_type == "relative":
            dispatch_timing = specific_timing.get("timings", [])
        else:
            dispatch_timing_list = specific_timing.get("timings", [])
            # 从右往左找到第一个"_"，取后面的部分转成数字
            # 默认flow_id满足routing_key = f"{self.params.task_id}_{operator_name}_{round_idx}"
            current_round = int(flow_id.rsplit('_', 1)[1])
            try:
                dispatch_timing = dispatch_timing_list[current_round]
            except:
                return [], [], []


        setting_amounts = specific_timing.get("amounts", [])
        dispatch_amount = [int(setting_amount) for setting_amount in setting_amounts]

        if len(dispatch_timing) != len(dispatch_amount):
            return [], [], []
        if len(dispatch_timing) == 0:
            return [], [], []

        drop_simulation = specific_timing.get("drop_simulation", {})
        if drop_simulation:
            if len(drop_simulation) != 1:  # 只能有一种drop的策略，不能存在两种
                return [], [], []
            drop_simulation_list = self._generate_drop_simulation_list(
                dispatch_amount=dispatch_amount,
                drop_simulation=drop_simulation
            )
        else:
            drop_simulation_list = [[]] * len(dispatch_amount)

        if time_type == "absolute":
            # 绝对时间点发送设定量
            # 转换dispatch_timing为datatime格式
            date_format = "%Y-%m-%d %H:%M:%S"
            dispatch_timing = [datetime.strptime(_timing, date_format) for _timing in dispatch_timing]
            # 获取当前时间
            current_time = datetime.now()
            current_time_ms = float(f"0.{current_time.microsecond}") #获取毫秒时间

            utc_timezone = pytz.timezone(time_zone)
            current_time_zone = current_time.astimezone(utc_timezone).strftime("%Y-%m-%d %H:%M:%S")
            current_time_object = datetime.strptime(current_time_zone, date_format)
            # 计算时间差
            dispatch_timing_seconds = [(_timing - current_time_object).total_seconds() for _timing in dispatch_timing]

            # 整体排序
            paired_lists = list(zip(dispatch_timing_seconds, dispatch_amount, drop_simulation_list))
            sorted_pairs = sorted(paired_lists, key=lambda x: x[0])
            dispatch_timing_seconds, dispatch_amount, drop_simulation_list = zip(*sorted_pairs)
            dispatch_timing_seconds, dispatch_amount, drop_simulation_list = \
                list(dispatch_timing_seconds), list(dispatch_amount), list(drop_simulation_list)

            # print(f"整体排序后: dispatch_timing_seconds = {dispatch_timing_seconds}")
            # print(f"整体排序后: dispatch_amount = {dispatch_amount}")
            # print(f"整体排序后: drop_simulation_list = {drop_simulation_list}")

            # 去除掉dispatch_timing小于0的部分
            valid_start_index = -1
            for i in range(len(dispatch_timing_seconds)):
                if dispatch_timing_seconds[i] >= 0:
                    valid_start_index = i
                    break

            if valid_start_index == -1:
                return [], [], []

            if valid_start_index > 0:
                dispatch_timing_seconds, dispatch_amount, drop_simulation_list = \
                    dispatch_timing_seconds[valid_start_index:], \
                    dispatch_amount[valid_start_index:], \
                    drop_simulation_list[valid_start_index:]

            # 更新
            dispatch_timing = [0] * len(dispatch_timing_seconds)
            for i in range(len(dispatch_timing_seconds)):
                if i == 0:
                    dispatch_timing[i] = dispatch_timing_seconds[0] - round(current_time_ms, 2)
                else:
                    dispatch_timing[i] = dispatch_timing_seconds[i] - dispatch_timing_seconds[i-1]
        else:
            pass

        return dispatch_timing, dispatch_amount, drop_simulation_list



    def _specific_interval_analysis(self,
                                    total_dispatch_amount: int,
                                    specific_interval: Dict[str, Any],
                                    flow_id: str):
        time_type = specific_interval.get("time_type", "relative")
        time_zone = specific_interval.get("time_zone", "Asia/Shanghai")

        # 处理absolute类型多轮执行的问题
        if time_type == "relative":
            intervals = specific_interval.get("intervals", [])
        else:
            intervals_list = specific_interval.get("intervals", [])
            # 从右往左找到第一个"_"，取后面的部分转成数字
            # 默认flow_id满足routing_key = f"{self.params.task_id}_{operator_name}_{round_idx}"
            current_round = int(flow_id.rsplit('_', 1)[1])
            try:
                intervals = intervals_list[current_round]
            except:
                return [], [], []


        # TODO: 需要在参数校验时确认intervals的区间之间是连续的; 相对时间单位为秒、绝对时间需精确到秒; 相对时间时需要为非负整数。
        dispatch_rules = specific_interval.get("dispatch_rules", {})
        domains = dispatch_rules.get("domains", [])
        functions = dispatch_rules.get("functions", [])
        drop_simulation = specific_interval.get("drop_simulation", {})

        if len(intervals) != len(domains) or len(domains) != len(functions):
            return [], [], []
        if len(intervals) == 0:
            return [], [], []
        if drop_simulation:
            if len(drop_simulation) != 1:  # 只能有一种drop的策略，不能存在两种
                return [], [], []

        if time_type == "relative":
            dispatch_timing, dispatch_amount, drop_simulation_list = self._get_interval_params(
                total_dispatch_amount = total_dispatch_amount,
                intervals = intervals,
                domains = domains,
                functions = functions,
                drop_simulation = drop_simulation
            )
            return dispatch_timing, dispatch_amount, drop_simulation_list

        elif time_type == "absolute":
            # 将绝对时间改成相对时间, 调整intervals
            date_format = "%Y-%m-%d %H:%M:%S"
            intervals_absolute = intervals
            intervals = []
            for i in range(len(intervals_absolute)):
                start_time = datetime.strptime(intervals_absolute[i][0], date_format)
                end_time = datetime.strptime(intervals_absolute[i][1], date_format)
                if i == 0:
                    interval_start = 0
                    interval_end = int((end_time - start_time).total_seconds())
                else:
                    pre_end_time = datetime.strptime(intervals_absolute[i-1][1], date_format)
                    interval_start = int((start_time - pre_end_time).total_seconds()) + intervals[i-1][1]
                    interval_end = int((end_time - start_time).total_seconds()) + interval_start
                intervals.append([interval_start, interval_end]) # 需要在参数校验时校验intervals之间的值是逐渐增长的
            # print(f"[_specific_interval_analysis]: intervals = {intervals}")
            # 获取参数
            dispatch_timing, dispatch_amount, drop_simulation_list = self._get_interval_params(
                total_dispatch_amount=total_dispatch_amount,
                intervals=intervals,
                domains=domains,
                functions=functions,
                drop_simulation=drop_simulation
            )
            # print(f"[_specific_interval_analysis]: dispatch_timing = {dispatch_timing}")
            # print(f"[_specific_interval_analysis]: dispatch_amount = {dispatch_amount}")
            # print(f"[_specific_interval_analysis]: drop_simulation_list = {drop_simulation_list}")
            # 修正起始值
            current_time = datetime.now()
            current_time_ms = float(f"0.{current_time.microsecond}")  # 获取毫秒时间
            utc_timezone = pytz.timezone(time_zone)
            current_time_zone = current_time.astimezone(utc_timezone).strftime("%Y-%m-%d %H:%M:%S")
            current_time_object = datetime.strptime(current_time_zone, date_format)
            start_time = datetime.strptime(intervals_absolute[0][0], date_format)
            dispatch_timing[0] = int((start_time - current_time_object).total_seconds()) - round(current_time_ms, 2)

            # TODO: 过时处理
            # print(f"过时处理")
            dispatch_timing_seconds = [0] * len(dispatch_timing)
            dispatch_timing_seconds[0] = dispatch_timing[0]
            for i in range(1, len(dispatch_timing_seconds)):
                dispatch_timing_seconds[i] = dispatch_timing_seconds[i-1] + dispatch_timing[i]
            # print(f"过时处理, dispatch_timing_seconds = {dispatch_timing_seconds}")
            # 去除掉dispatch_timing小于0的部分
            valid_start_index = -1
            for i in range(len(dispatch_timing_seconds)):
                if dispatch_timing_seconds[i] >= 0:
                    valid_start_index = i
                    break
            if valid_start_index == -1:
                return [], [], []
            elif valid_start_index > 0:
                dispatch_timing, dispatch_amount, drop_simulation_list = \
                    dispatch_timing[valid_start_index:], \
                    dispatch_amount[valid_start_index:], \
                    drop_simulation_list[valid_start_index:]
                dispatch_timing[0] = dispatch_timing_seconds[valid_start_index]
            else:
                pass
            # print(f"过时处理, valid_start_index = {valid_start_index}")

            return dispatch_timing, dispatch_amount, drop_simulation_list

    def _generate_drop_simulation_list(self,
                                       dispatch_amount: List[int],
                                       drop_simulation: Dict[str, Any]):
        # 目前drop_simulation里只能有"drop_probability"或"drop_amounts"二者之一，有关strategy的参数校验需要完成校验
        drop_simulation_list = []
        if "drop_probability" in drop_simulation:
            # 有关strategy的参数校验, 需要核实drop_probability和dispatch_amount的维度相同, 核实drop_probability范围
            drop_probability = drop_simulation.get("drop_probability", [])
            for probability, amount in zip(drop_probability, dispatch_amount):
                drop_idx = []
                if probability == 0:
                    pass
                elif probability == 1:
                    drop_idx = [i for i in range(int(amount))]
                elif (probability > 0) and (probability < 1):  # 0 < probability < 1
                    for i in range(int(amount)):
                        if random.random() < probability:
                            drop_idx.append(i)
                drop_simulation_list.append(drop_idx)
            return drop_simulation_list

        if "drop_amounts" in drop_simulation:
            drop_amounts = drop_simulation.get("drop_amounts", [])
            # 有关strategy的参数校验, 需要核实drop_amounts和dispatch_amount的维度相同, 核实drop_amounts值的范围
            for drop_amount, amount in zip(drop_amounts, dispatch_amount):
                drop_idx = []
                if drop_amount == 0:
                    pass
                elif (drop_amount > 0) and (int(drop_amount) < int(amount)):  # 0 < drop_amount < amount
                    drop_idx = random.sample(range(int(amount)), int(drop_amount))
                    drop_idx = sorted(drop_idx)
                else:
                    drop_idx = [i for i in range(int(amount))]
                drop_simulation_list.append(drop_idx)
            return drop_simulation_list

        return []


    def _get_interval_params(self,
                             total_dispatch_amount: int,
                             intervals: List[List[int]],
                             domains: List[List[float]],
                             functions: List[str],
                             drop_simulation: Dict[str, Any]
                             ):
        """
        根据intervals, domains, functions，获取dispatch信息
        """
        # 相对时间发送设定量
        t_list = []  # 每个函数的dispatch时间点，List[List[int]]
        area_list = []  # 每个函数在对应dispatch时间点的面积，List[List[float]]
        # 逐个(interval, domain, func_string)确认，获取t_list、area_list
        for interval, domain, func_string in zip(intervals, domains, functions):
            # interval: [0, 1, 2, ..., 10],  domain: [0, 0.628, ..., 6.28], func_string: "math.sin(t)+1"
            interval_length = interval[1] - interval[0]
            domain_length = domain[1] - domain[0]
            # t_interval_list、t_domain_list、y_list的维度是len(dispatch_timing)+1, 这样才能计算出正确的area
            t_interval_list = list(range(interval[0], interval[1] + 1))
            t_domain_list = [
                domain[0] + domain_length / interval_length * (t_interval - t_interval_list[0])
                for t_interval in t_interval_list
            ]
            interval_area_list = []
            for i in range(len(t_domain_list) - 1):
                t_linspace = np.linspace(t_domain_list[i], t_domain_list[i + 1],
                                         num=AREA_CALCULATION_NUM + 1).tolist()  # 保证间隔数量为AREA_CALCULATION_NUM
                y_linspace = [eval(func_string) for t in t_linspace]  # 根据函数计算函数值
                interval_area = 0
                for j in range(1, len(y_linspace)):
                    # 梯形面积, interval的长度默认为1(代表1s)，
                    area_small_interval = 0.5 * (y_linspace[j] + y_linspace[j - 1]) * (1 / AREA_CALCULATION_NUM)
                    if area_small_interval > 0:
                        interval_area += area_small_interval  # 计算总面积
                interval_area_list.append(interval_area)

            t_list.append(t_interval_list[:-1])  # t_list
            area_list.append(interval_area_list)  # 记录需要发送的信息所对应的area，后续需要进行转换

        # 根据total_dispatch_amount，将area_list转化为dispatch_amount
        total_area_list = [sum(area_list_element) for area_list_element in area_list]
        total_area = sum(total_area_list)
        # 如果计算的总面积小于0，则直接退出
        if total_area <= 0:
            return [], [], []

        amount_list = [round(total_area_element / total_area * total_dispatch_amount) for total_area_element in
                       total_area_list]
        amount_list[-1] = total_dispatch_amount - sum(amount_list[:-1])
        actual_send_list = []
        for idx in range(len(amount_list)):
            send_amount = amount_list[idx]
            send_nums = [0]
            send_nums.extend([area_element / total_area_list[idx] * send_amount for area_element in area_list[idx]])

            tmp_nums = [0]  # 残差列表
            actual_send_num_list = [0]
            # 需要对每个interval的发送总量进行整合处理
            for i in range(1, len(send_nums)):
                tmp = tmp_nums[i - 1] + send_nums[i]
                if round(tmp) > 0:
                    actual_send_num_list.append(round(tmp))
                    tmp_nums.append(tmp - round(tmp))
                else:
                    actual_send_num_list.append(0)
                    tmp_nums.append(tmp)
            actual_send_num_list = actual_send_num_list[1:]
            actual_send_list.append(actual_send_num_list)

        # 对drop_simulation进行更新
        # drop_probability
        if "drop_probability" in drop_simulation:
            drop_probability = drop_simulation.get("drop_probability", [])
            # TODO: len(actual_send_list) 应该等于 len(drop_probability)
            drop_probability_new = []
            for idx, element in enumerate(actual_send_list):
                drop_probability_new.extend(
                    [drop_probability[idx]] * len(element)
                )
            drop_simulation["drop_probability"] = drop_probability_new

        # drop_amounts
        elif "drop_amounts" in drop_simulation:
            drop_amounts = drop_simulation.get("drop_amounts", [])
            drop_amounts_new = []
            for idx, element in enumerate(actual_send_list):
                element_sum = sum(element)
                drop_amount = drop_amounts[idx]
                if drop_amount == 0:
                    drop_amounts_new.extend(
                        [0] * len(element)
                    )
                elif drop_amount == element_sum:
                    drop_amounts_new.extend(element)
                elif (drop_amount > 0) and (drop_amount < element_sum):
                    drop_idx = random.sample(range(element_sum), drop_amount)
                    drop_idx = sorted(drop_idx)
                    drop_amount_list = []
                    num_ind = -1
                    for i in range(len(element)):
                        drop_num = 0
                        if element[i] != 0:
                            for j in range(element[i]):
                                num_ind = num_ind + 1
                                if num_ind in drop_idx:
                                    drop_num += 1
                        drop_amount_list.append(drop_num)
                    drop_amounts_new.extend(drop_amount_list)
            drop_simulation["drop_amounts"] = drop_amounts_new

        # 拼出dispatch_timing, dispatch_amount, drop_simulation_list
        dispatch_timing_origin, dispatch_amount = [], []
        for list_timing, list_amount in zip(t_list, actual_send_list):
            dispatch_timing_origin.extend(list_timing)
            dispatch_amount.extend(list_amount)
        # 对dispatch_timing进行修正
        dispatch_timing = [0] * len(dispatch_timing_origin)
        for i in range(len(dispatch_timing_origin)):
            if i == 0:
                dispatch_timing[i] = dispatch_timing_origin[0]
            else:
                dispatch_timing[i] = dispatch_timing_origin[i] - dispatch_timing_origin[i - 1]
        if drop_simulation:
            drop_simulation_list = self._generate_drop_simulation_list(
                dispatch_amount=dispatch_amount,
                drop_simulation=drop_simulation
            )
        else:
            drop_simulation_list = [[]] * len(dispatch_amount)

        return dispatch_timing, dispatch_amount, drop_simulation_list