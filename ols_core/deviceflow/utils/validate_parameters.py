import json
import pytz
import logging
import math
from datetime import datetime
from enum import Enum

class ValidateParameters:
    def __init__(self):
        pass

    def check_params_of_notify_start(self, request):
        compute_resource = request.compute_resource
        logging.info(f"check_params_of_notify_start, strategy {request.strategy}")
        try:
            strategy = json.loads(request.strategy)
        except Exception:
            return False, "strategy not json format"

        if not hasattr(ComputeResource, compute_resource):
            return False, "compute resource error"
        return self.check_strategy(strategy)

    def check_strategy(self, strategy):
        real_time_dispatch_strategy = strategy.get("real_time_dispatch", {})
        use_time_dispatch_strategy = real_time_dispatch_strategy.get("use_strategy", False)
        flow_dispatch_strategy = strategy.get("flow_dispatch", {})
        use_flow_dispatch_strategy = flow_dispatch_strategy.get("use_strategy", False)

        if use_time_dispatch_strategy == use_flow_dispatch_strategy:
            return False, "Must use one strategy"
        if use_time_dispatch_strategy:
            check_real_time_result, check_real_time_message = self.check_real_time_dispatch_strategy(real_time_dispatch_strategy)
            if not check_real_time_result:
                return check_real_time_result, check_real_time_message
        elif use_flow_dispatch_strategy:
            check_flow_result, check_flow_result_message = self.check_flow_dispatch_strategy(flow_dispatch_strategy)
            if not check_flow_result:
                return check_flow_result, check_flow_result_message
        return True, "Pass"


    def check_real_time_dispatch_strategy(self, real_time_dispatch_strategy):
        logging.info(f"check_real_time_dispatch_strategy")
        # check drop_probability
        drop_probability = real_time_dispatch_strategy.get("drop_simulation", {}).get("drop_probability", -1)
        if drop_probability != -1 and not 0 <= drop_probability <= 1:
            return False, "drop probability must in [0,1]"
        return True, "Pass"

    def check_flow_dispatch_strategy(self, flow_dispatch_strategy):
        total_dispatch_amount = flow_dispatch_strategy.get("total_dispatch_amount", -1)

        use_specific_timing = flow_dispatch_strategy.get("specific_timing", {}).get("use", False)
        use_specific_interval = flow_dispatch_strategy.get("specific_interval", {}).get("use", False)
        if use_specific_timing == use_specific_interval:
            return False, "Must use one specific strategy"

        # check time_type and time_zone
        if use_specific_timing:
            time_type = flow_dispatch_strategy.get("specific_timing", {}).get("time_type", "")
            time_zone = flow_dispatch_strategy.get("specific_timing", {}).get("time_zone", "")
        elif use_specific_interval:
            time_type = flow_dispatch_strategy.get("specific_interval", {}).get("time_type", "")
            time_zone = flow_dispatch_strategy.get("specific_interval", {}).get("time_zone", "")
        if time_type == "":
            return False, "time type error"
        elif not hasattr(StrategyTimeType, time_type):
            return False, "time type error, absolute or relative need"

        if time_type == StrategyTimeType.absolute.name:
            if time_zone == "":
                return False, "time zone error"
            # check timezone format
            else:
                try:
                    pytz.timezone(time_zone)
                except pytz.exceptions.UnknownTimeZoneError:
                    return False, "time zone error, format must be supported by pytz"

        # check drop probability and amount
        if use_specific_timing:
            drop_probability = flow_dispatch_strategy.get("specific_timing", {}).get("drop_simulation", {}).get("drop_probability", [])
            drop_amounts = flow_dispatch_strategy.get("specific_timing", {}).get("drop_simulation", {}).get("drop_amounts", [])
        elif use_specific_interval:
            drop_probability = flow_dispatch_strategy.get("specific_interval", {}).get("drop_simulation", {}).get("drop_probability", [])
            drop_amounts = flow_dispatch_strategy.get("specific_interval", {}).get("drop_simulation", {}).get("drop_amounts", [])
        if drop_probability and drop_amounts:
            return False, "drop probability and drop amounts can't be set at the same time"
        if drop_probability:
            for dp in drop_probability:
                if not 0 <= dp <= 1:
                    return False, "drop probability must in [0,1]"
        elif drop_amounts:
            if total_dispatch_amount < sum(drop_amounts):
                return False, "drop amounts sum > total dispatch amount"

        # check specific timing
        if use_specific_timing:
            #logging.info(f"check_flow_dispatch_strategy use_specific_timing")
            amounts = flow_dispatch_strategy.get("specific_timing", {}).get("amounts", [])

            # 对于absolute的多轮场景，需要额外处理一下, by wjj 2024/11/18
            if time_type == StrategyTimeType.relative.name:
                timings = flow_dispatch_strategy.get("specific_timing", {}).get("timings", [])
                timings_list = [timings] #只是为了统一格式
            else:
                timings_list = flow_dispatch_strategy.get("specific_timing", {}).get("timings", [])

            for timings in timings_list:
                try:
                    # check size
                    if not len(amounts) == len(timings):
                        return False, "amounts and timings must have the same size"
                    if drop_probability:
                        if not len(amounts) == len(drop_probability):
                            return False, "amounts, timings and drop_probability must have the same size"
                    elif drop_amounts:
                        if not len(amounts) == len(drop_amounts):
                            return False, "amounts, timings and drop_amounts must have the same size"
                    # check amounts
                    if total_dispatch_amount != sum(amounts):
                        return False, "amounts not equal total dispatch amount"

                    # check time format
                    if time_type == StrategyTimeType.absolute.name:
                        time_format = "%Y-%m-%d %H:%M:%S"
                        for time in timings:
                            try:
                                datetime.strptime(time, time_format)
                            except ValueError:
                                return False, "absolute time format error, must %Y-%m-%d %H:%M:%S"
                    elif time_type == StrategyTimeType.relative.name:
                        for time in timings:
                            try:
                                if time < 0:
                                    return False, "relative time format error, must >= 0"
                            except Exception:
                                return False, "relative time format error, must figure"
                except Exception as e:
                    return False, f"{e}"

        # check specific interval
        elif use_specific_interval:
            # check intervals
            # 对于absolute的多轮场景，需要额外处理一下, by wjj 2024/11/18
            if time_type == StrategyTimeType.relative.name:
                intervals = flow_dispatch_strategy.get("specific_interval", {}).get("intervals", [])
                intervals_list = [intervals] # 只是为了统一格式
            else:
                intervals_list = flow_dispatch_strategy.get("specific_interval", {}).get("intervals", [])

            for intervals in intervals_list:
                try:
                    flattened_intervals_list = [x for inner_list in intervals for x in inner_list]
                    #logging.info(f"check_flow_dispatch_strategy use_specific_interval, flattened_intervals_list {flattened_intervals_list}")
                    if time_type == StrategyTimeType.absolute.name:
                        time_format = "%Y-%m-%d %H:%M:%S"
                        try:
                            time_objects = [datetime.strptime(t, time_format) for t in flattened_intervals_list]
                            absolute_times = [t.timestamp() for t in time_objects]
                            #logging.info(f"check_flow_dispatch_strategy use_specific_interval, absolute_times {absolute_times}")
                            is_increasing_intervals = True
                            for i in range(len(absolute_times) - 1):
                                if i % 2 == 0:
                                    if absolute_times[i] >= absolute_times[i + 1]:
                                        is_increasing_intervals = False
                                        break
                                else:
                                    if absolute_times[i] > absolute_times[i + 1]:
                                        is_increasing_intervals = False
                                        break
                            #logging.info(f"check_flow_dispatch_strategy use_specific_interval, is_increasing_intervals: {is_increasing_intervals}")
                            if not is_increasing_intervals:
                                return False, "absolute time value error"
                        except ValueError:
                            return False, "absolute time format error, must %Y-%m-%d %H:%M:%S"
                    elif time_type == StrategyTimeType.relative.name:
                        is_natural_number = all(flattened_intervals_list[i] >=0 for i in range(len(flattened_intervals_list) - 1))
                        if not is_natural_number:
                            return False, "relative time format error, must >= 0"
                        #[[1,2],[2,3]] pass, [[1,1],[2,3]] fail
                        is_increasing_intervals = True
                        for i in range(len(flattened_intervals_list) - 1):
                            if i%2 == 0:
                                if flattened_intervals_list[i] >= flattened_intervals_list[i + 1]:
                                    is_increasing_intervals = False
                                    break
                            else:
                                if flattened_intervals_list[i] > flattened_intervals_list[i + 1]:
                                    is_increasing_intervals = False
                                    break
                        #logging.info(f"check_flow_dispatch_strategy use_specific_interval, is_increasing_intervals: {is_increasing_intervals}")
                        if not is_increasing_intervals:
                            return False, "relative time value error"

                    # check dispatch_rules
                    # check domains and functions
                    domains = flow_dispatch_strategy.get("specific_interval", {}).get("dispatch_rules", {}).get("domains", [])
                    functions = flow_dispatch_strategy.get("specific_interval", {}).get("dispatch_rules", {}).get("functions", [])
                    try:
                        # check size
                        if not len(intervals) == len(domains) == len(functions):
                            return False, "intervals, domains and functions must have the same size"
                        if drop_probability:
                            if not len(intervals) == len(drop_probability):
                                return False, "intervals, domains, functions and drop_probability must have the same size"
                        elif drop_amounts:
                            if not len(intervals) == len(drop_amounts):
                                return False, "intervals, domains, functions and drop_amounts must have the same size"

                        for i in range(0, len(domains)):
                            if domains[i][0] >= domains[i][1]:
                                return False, "domains right value must be greater than the left value"
                            t = domains[i][0]
                            expression = functions[i]
                            result = eval(expression)
                            #logging.info(f"check_flow_dispatch_strategy check domains and functions, expression: {expression}, result: {result}")
                    except Exception as e:
                        return False, "domains or functions error, variable must be t"

                except Exception as e:
                    return False, f"{e}"

        return True, "Pass"

class ComputeResource(Enum):
    logical_simulation = 1
    device_simulation = 2

class StrategyTimeType(Enum):
    absolute = 1
    relative = 2
