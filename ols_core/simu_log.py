import os
import yaml
import logging
import sqlalchemy
import pandas as pd

from sqlalchemy.exc import OperationalError
from pymysql.err import OperationalError as PyMySQLOperationalError

logging.getLogger('sqlalchemy').setLevel(logging.CRITICAL)
logging.getLogger('pymysql').setLevel(logging.CRITICAL)

class Logger:
    def __init__(self):
        self._user = ""
        self._password = ""
        self._host = ""
        self._port = ""
        self._database = ""
        self._table = ""
        self._engine = ""
        self._logger = None

    def set_logger(self, log_yaml, logfile=""):
    # local
        try:
            if logfile=="":
                filepath = os.path.abspath(os.path.dirname(__file__))
                logpath = os.path.abspath(os.path.join(filepath, '../..')) + "/log"
                os.makedirs(logpath, exist_ok=True, mode=0o777)
                logfile = f"{logpath}/log_simulation.log"
            self._logger = logging.getLogger("simulation")
            self._logger.setLevel(logging.INFO)
            if not self._logger.handlers:
                handler = logging.FileHandler(filename=logfile, mode="a")
                formatter = logging.Formatter(
                    fmt="[%(asctime)s] | [%(levelname)s] | %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
                handler.setFormatter(formatter)
                handler.setLevel(logging.INFO)
                self._logger.addHandler(handler)
        except Exception as e:
            print(f"[ols_logger]: set_logger() from log_yaml={log_yaml} failed, because of {e}")
            pass
    # sql
        try:
            with open(log_yaml, 'r') as file:
                log_config = yaml.load(file, Loader=yaml.FullLoader)
            self._user = log_config["user"]
            self._password = log_config["password"]
            self._host = log_config["host"]
            self._port = str(log_config["port"])
            self._database = log_config["database"]
            self._table = log_config["table"]
            self._initialize_db()
            # self._engine = sqlalchemy.create_engine(
            #     f"mysql+pymysql://{self._user}:{self._password}@{self._host}:{self._port}/{self._database}"
            # )
        except Exception as e:
            print(f"[ols_logger]: set_logger() from log_yaml={log_yaml} failed, because of {e}")
            pass

    def _initialize_db(self):
        self._engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self._user}:{self._password}@{self._host}:{self._port}/{self._database}"
        )

    def fill_default_values(self, task_id, other_number, system_name, module_name, message):
        log_dict = {
            "task_id": [task_id],
            "other_number": [other_number],
            "system_name": [system_name],
            "module_name": [module_name],
            "message": [message],
        }
        return log_dict

    def info(self, task_id, system_name, module_name, message, other_number=""):
        # local
        try:
            self._logger.info(f"{module_name}\t | {message}")
        except:
            pass
        # sql
        try:
            log_dict = self.fill_default_values(
                task_id=task_id, other_number=other_number,
                system_name=system_name, module_name=module_name,
                message=message
            )
            log_dict.update({"log_type": ["INFO"]})
            log_df = pd.DataFrame.from_dict(log_dict)
            log_df.to_sql(name=self._table, con=self._engine, if_exists="append", index=False)
        except (OperationalError, PyMySQLOperationalError, ConnectionResetError) as e:
            self._initialize_db()
            try:
                log_dict = self.fill_default_values(
                    task_id=task_id, other_number=other_number,
                    system_name=system_name, module_name=module_name,
                    message=message
                )
                log_dict.update({"log_type": ["INFO"]})
                log_df = pd.DataFrame.from_dict(log_dict)
                log_df.to_sql(name=self._table, con=self._engine, if_exists="append", index=False)
            except:
                pass
        except:
            pass

    def error(self, task_id, system_name, module_name, message, other_number=""):
        # local
        try:
            self._logger.error(f"{module_name}\t | {message}")
        except:
            pass
        # sql
        try:
            log_dict = self.fill_default_values(
                task_id=task_id, other_number=other_number,
                system_name=system_name, module_name=module_name,
                message=message
            )
            log_dict.update({"log_type": ["ERROR"]})
            log_df = pd.DataFrame.from_dict(log_dict)
            log_df.to_sql(name=self._table, con=self._engine, if_exists="append", index=False)
        except (OperationalError, PyMySQLOperationalError, ConnectionResetError) as e:
            self._initialize_db()
            try:
                log_dict = self.fill_default_values(
                    task_id=task_id, other_number=other_number,
                    system_name=system_name, module_name=module_name,
                    message=message
                )
                log_dict.update({"log_type": ["ERROR"]})
                log_df = pd.DataFrame.from_dict(log_dict)
                log_df.to_sql(name=self._table, con=self._engine, if_exists="append", index=False)
            except:
                pass
        except:
            pass

    def warning(self, task_id, system_name, module_name, message, other_number=""):
        # local
        try:
            self._logger.warning(f"{module_name}\t | {message}")
        except:
            pass
        # sql
        try:
            log_dict = self.fill_default_values(
                task_id=task_id, other_number=other_number,
                system_name=system_name, module_name=module_name,
                message=message
            )
            log_dict.update({"log_type": ["WARNING"]})
            log_df = pd.DataFrame.from_dict(log_dict)
            log_df.to_sql(name=self._table, con=self._engine, if_exists="append", index=False)
        except (OperationalError, PyMySQLOperationalError, ConnectionResetError) as e:
            self._initialize_db()
            try:
                log_dict = self.fill_default_values(
                    task_id=task_id, other_number=other_number,
                    system_name=system_name, module_name=module_name,
                    message=message
                )
                log_dict.update({"log_type": ["WARNING"]})
                log_df = pd.DataFrame.from_dict(log_dict)
                log_df.to_sql(name=self._table, con=self._engine, if_exists="append", index=False)
            except:
                pass
        except:
            pass


if __name__ == '__main__':
    logger = Logger()
    logger.set_logger(log_yaml="config/repo_log.yaml")
    logger.info(task_id="ols_sql_test", system_name="TaskMgr", module_name="task_manager",
        message = "INFO_记录日志"
    )
    logger.error(task_id="ols_sql_test", system_name="TaskMgr", module_name="task_manager",
        message = "ERROR_记录日志"
    )
    logger.warning(task_id="ols_sql_test", system_name="TaskMgr", module_name="task_manager",
        message = "WARNING_记录日志"
    )