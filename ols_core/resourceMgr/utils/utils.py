import os
import yaml
import pytz
import sqlalchemy
import pandas as pd
from ols.simu_log import Logger
from sqlalchemy import MetaData, Table, update, select, insert, delete, and_

from sqlalchemy.exc import OperationalError
from pymysql.err import OperationalError as PyMySQLOperationalError

class ResTableRepo:
    def __init__(self, yaml_path:str):
        with open(yaml_path, 'r') as file:
            res_table_config = yaml.load(file, Loader=yaml.FullLoader)
        self._user = res_table_config["user"]
        self._password = res_table_config["password"]
        self._host = res_table_config["host"]
        self._port = str(res_table_config["port"])
        self._database = res_table_config["database"]
        self._table_name = res_table_config["table"]
        self._initialize_db()
        #log
        filepath = os.path.abspath(os.path.dirname(__file__))   # resource_manager.py文件所在的绝对路径
        olspath = os.path.abspath(os.path.join(filepath, '../..')) # ols所在文件夹的绝对路径
        self._logger = Logger()
        self._logger.set_logger(log_yaml = olspath + "/config/repo_log.yaml")

    def _initialize_db(self):
        self._engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self._user}:{self._password}@{self._host}:{self._port}/{self._database}"
        )
        self._metadata = MetaData()
        self._table = Table(self._table_name, self._metadata, autoload=True, autoload_with=self._engine)

    # 新增数据
    def insert_data(self, value):
        try:
            insert_sql = insert(self._table).values(value)
            self._engine.execute(insert_sql)
            return True
        except (OperationalError, PyMySQLOperationalError, ConnectionResetError) as e:
            # Retry重连
            self._logger.warning(task_id="", system_name="ResMgr", module_name="ResTableRepo",
                                 message=f"[insert_data]: OperationalError encountered, reinitializing database connection")
            self._initialize_db()
            try:
                insert_sql = insert(self._table).values(value)
                self._engine.execute(insert_sql)
                return True
            except Exception as e:
                self._logger.error(task_id="", system_name="ResMgr", module_name="ResTableRepo",
                                   message=f"[insert_data]: Set value={value}  failed, because of {e}")
                return False
        except Exception as e:
            self._logger.error(task_id = "", system_name = "ResMgr", module_name = "ResTableRepo",
                message = f"[insert_data]: Set value={value}  failed, because of {e}")
            return False

    # 根据task_id删除数据
    def delete_by_taskid(self, task_id: str):
        try:
            delete_sql = delete(self._table).where(self._table.c.task_id == task_id)
            result = self._engine.execute(delete_sql)
            return True
        except (OperationalError, PyMySQLOperationalError, ConnectionResetError) as e:
            # Retry重连
            self._logger.warning(task_id = task_id, system_name="ResMgr", module_name="ResTableRepo",
                               message=f"[delete_by_taskid]: OperationalError encountered, reinitializing database connection for task_id={task_id}")
            self._initialize_db()
            try:
                delete_sql = delete(self._table).where(self._table.c.task_id == task_id)
                result = self._engine.execute(delete_sql)
                return True
            except Exception as e:
                self._logger.error(task_id=task_id, system_name="ResMgr", module_name="ResTableRepo",
                                   message=f"[delete_by_taskid]: delete task_id={task_id} failed, because of {e}")
                return False
        except Exception as e:
            self._logger.error(task_id = task_id, system_name = "ResMgr", module_name = "ResTableRepo",
                message = f"[delete_by_taskid]: delete task_id={task_id} failed, because of {e}")
            return False

    def get_jsons_by_item_values(self, item: str, values: list):
        try:
            select_query = select([self._table]).where(self._table.c[item].in_(values))
            result = self._engine.execute(select_query).fetchall()
            json_list = []
            if result:
                for row in result:
                    row_dict = row._asdict()
                    json_list.append(row_dict)
            return json_list
        except (OperationalError, PyMySQLOperationalError, ConnectionResetError) as e:
            # Retry重连
            self._logger.warning(task_id = "", system_name="ResMgr", module_name="ResTableRepo",
                               message=f"[get_jsons_by_item_values]: OperationalError encountered, reinitializing database connection")
            self._initialize_db()
            try:
                select_query = select([self._table]).where(self._table.c[item].in_(values))
                result = self._engine.execute(select_query).fetchall()
                json_list = []
                if result:
                    for row in result:
                        row_dict = row._asdict()
                        json_list.append(row_dict)
                return json_list
            except Exception as e:
                self._logger.error(task_id="", system_name="ResMgr", module_name="ResTableRepo",
                                   message=f"[get_data_by_item_value]: Get item={item} and values={values} failed, because of {e}")
                return []
        except Exception as e:
            self._logger.error(task_id = "", system_name = "ResMgr", module_name = "ResTableRepo",
                message = f"[get_data_by_item_value]: Get item={item} and values={values} failed, because of {e}")
            return []

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
            self._logger.warning(task_id="", system_name="ResMgr", module_name="ResTableRepo",
                                 message=f"[get_data_by_item_values]: OperationalError encountered, reinitializing database connection")
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
                self._logger.error(task_id="", system_name="ResMgr", module_name="ResTableRepo",
                                   message=f"[get_data_by_item_value]: Get item={item} and values={values} failed, because of {e}")
                return pd.DataFrame()
        except Exception as e:
            self._logger.error(task_id = "", system_name = "ResMgr", module_name = "ResTableRepo",
                message = f"[get_data_by_item_value]: Get item={item} and values={values} failed, because of {e}")
            return pd.DataFrame()

    def set_item_value(self, task_id:str, item:str, value):
        try:
            update_query = update(self._table).where(self._table.c.task_id==task_id).values({item: value})
            self._engine.execute(update_query)
            return True
        except (OperationalError, PyMySQLOperationalError, ConnectionResetError) as e:
            # Retry重连
            self._logger.warning(task_id=task_id, system_name="ResMgr", module_name="ResTableRepo",
                               message=f"[set_item_value]: OperationalError encountered, reinitializing database connection for task_id = {task_id}")
            self._initialize_db()
            try:
                update_query = update(self._table).where(self._table.c.task_id == task_id).values({item: value})
                self._engine.execute(update_query)
                return True
            except Exception as e:
                self._logger.error(task_id=task_id, system_name="ResMgr", module_name="ResTableRepo",
                                   message=f"[set_task_status]: Set item={item} of task_id={task_id} failed, because of {e}")
                return False
        except Exception as e:
            self._logger.error(task_id = task_id, system_name = "ResMgr", module_name = "ResTableRepo",
                message = f"[set_task_status]: Set item={item} of task_id={task_id} failed, because of {e}")
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
            self._logger.warning(task_id=task_id, system_name="ResMgr", module_name="ResTableRepo",
                               message=f"[get_item_value]: OperationalError encountered, reinitializing database connection for task_id = {task_id}")
            self._initialize_db()
            try:
                select_query = select([self._table.c[item]]).where(self._table.c.task_id == task_id)
                result = self._engine.execute(select_query).fetchone()
                if result:
                    return result[0]
                else:
                    return None
            except Exception as e:
                self._logger.error(task_id=task_id, system_name="ResMgr", module_name="ResTableRepo",
                                   message=f"[get_item_value]: Get item={item} of task_id={task_id} failed, because of {e}")
                return None
        except Exception as e:
            self._logger.error(task_id = task_id, system_name = "ResMgr", module_name = "ResTableRepo",
                message = f"[get_item_value]: Get item={item} of task_id={task_id} failed, because of {e}")
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

