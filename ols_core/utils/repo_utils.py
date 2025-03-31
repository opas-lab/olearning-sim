import os
import yaml
import sqlalchemy
import pandas as pd
from ols.simu_log import Logger
from sqlalchemy import MetaData, Table, update, select, and_, insert, delete

from sqlalchemy.exc import OperationalError
from pymysql.err import OperationalError as PyMySQLOperationalError

# filepath
FILEPATH = os.path.abspath(os.path.dirname(__file__))   # repo_utils.py文件所在的绝对路径
OLSPATH = os.path.abspath(os.path.join(FILEPATH, '..')) # ols所在文件夹的绝对路径
# log
logger = Logger()
logger.set_logger(log_yaml = OLSPATH + "/config/repo_log.yaml")


class SqlDataBase(object):
    def __init__(self, yaml_path: str):
        with open(yaml_path, 'r') as file:
            table_config = yaml.load(file, Loader=yaml.FullLoader)
        self._user = table_config["user"]
        self._password = table_config["password"]
        self._host = table_config["host"]
        self._port = str(table_config["port"])
        self._database = table_config["database"]
        self._table_name = table_config["table"]
        self._initialize_db()

    def _initialize_db(self):
        self._engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self._user}:{self._password}@{self._host}:{self._port}/{self._database}"
        )
        self._metadata = MetaData()
        self._table = Table(self._table_name, self._metadata, autoload=True, autoload_with=self._engine)

    def get_data_by_item_values(self, item: str, values: list):
        """
        获取字段为item, 值为values(list)的所有数据条目
        """
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

    #TODO:可以用装饰器统一处理

    # 不依赖于task_id
    def set_item_value(
            self,
            identify_name: str, identify_value,
            item: str, value,
            log_params: dict = {}
    ):
        """
        在数据库里找字段identify_name的值为identify_value的数据条目，将其item字段设置为value，并赋予log日志字段log_params
        log_params = {"task_id": "aaa", "system_name": "Deviceflow", "module_name": "set_item_value"}
        """
        try:
            update_query = update(self._table).where(eval(f"self._table.c.{identify_name}")==identify_value).values({item: value})
            self._engine.execute(update_query)
            return True
        except (OperationalError, PyMySQLOperationalError, ConnectionResetError) as e:
            # Retry重连
            task_id = log_params.get("task_id", "")
            logger.warning(task_id=task_id, system_name="TaskMgr", module_name="TaskTableRepo",
                           message=f"OperationalError encountered, reinitializing database connection for task_id = {task_id}")
            self._initialize_db()
            try:
                update_query = update(self._table).where(
                    eval(f"self._table.c.{identify_name}") == identify_value).values({item: value})
                self._engine.execute(update_query)
                return True
            except Exception as e:
                task_id = log_params.get("task_id", "")
                system_name = log_params.get("system_name", "")
                module_name = log_params.get("module_name", "")
                logger.error(task_id=task_id, system_name=system_name, module_name=module_name,
                             message=f"[set_item_value]: Set item={item} of {identify_name}={identify_value} failed, because of {e}")
                return False
        except Exception as e:
            task_id = log_params.get("task_id", "")
            system_name = log_params.get("system_name", "")
            module_name = log_params.get("module_name", "")
            logger.error(task_id=task_id, system_name=system_name, module_name=module_name,
                         message=f"[set_item_value]: Set item={item} of {identify_name}={identify_value} failed, because of {e}")
            return False


    def get_item_value(self,  identify_name: str, identify_value, item: str, log_params: dict = {}):
        try:
            select_query = select([self._table.c[item]]).where(eval(f"self._table.c.{identify_name}") == identify_value)
            result = self._engine.execute(select_query).fetchone()
            if result:
                return result[0]
            else:
                return None
        except (OperationalError, PyMySQLOperationalError, ConnectionResetError) as e:
            # Retry重连
            task_id = log_params.get("task_id", "")
            logger.warning(task_id=task_id, system_name="TaskMgr", module_name="TaskTableRepo",
                           message=f"OperationalError encountered, reinitializing database connection for task_id = {task_id}")
            self._initialize_db()
            try:
                select_query = select([self._table.c[item]]).where(
                    eval(f"self._table.c.{identify_name}") == identify_value)
                result = self._engine.execute(select_query).fetchone()
                if result:
                    return result[0]
                else:
                    return None
            except Exception as e:
                task_id = log_params.get("task_id", "")
                system_name = log_params.get("system_name", "")
                module_name = log_params.get("module_name", "")
                logger.error(task_id=task_id, system_name=system_name, module_name=module_name,
                             message=f"[get_item_value]: Get item={item} of identify_name={identify_value} failed, because of {e}")
                return None
        except Exception as e:
            task_id = log_params.get("task_id", "")
            system_name = log_params.get("system_name", "")
            module_name = log_params.get("module_name", "")
            logger.error(task_id=task_id, system_name=system_name, module_name=module_name,
                        message=f"[get_item_value]: Get item={item} of identify_name={identify_value} failed, because of {e}")
            return None


    def check_task_in_database(self, identify_name: str, identify_value):
        try:
            select_query = select([self._table]).where(eval(f"self._table.c.{identify_name}") == identify_value)
            result = self._engine.execute(select_query).fetchone()
            if result:
                return True
            else:
                return False
        except (OperationalError, PyMySQLOperationalError, ConnectionResetError) as e:
            # Retry重连
            self._initialize_db()
            try:
                select_query = select([self._table]).where(eval(f"self._table.c.{identify_name}") == identify_value)
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

    def add_item(self, item: dict, log_params: dict = {}):
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
            task_id = log_params.get("task_id", "")
            logger.warning(task_id=task_id, system_name="TaskMgr", module_name="TaskTableRepo",
                           message=f"OperationalError encountered, reinitializing database connection for task_id = {task_id}")
            self._initialize_db()
            try:
                # 创建一个插入语句
                insert_stmt = insert(self._table).values(**item)
                # 执行插入操作
                self._engine.execute(insert_stmt)
                return True
            except Exception as e:
                task_id = log_params.get("task_id", "")
                system_name = log_params.get("system_name", "")
                module_name = log_params.get("module_name", "")
                logger.error(task_id=task_id, system_name=system_name, module_name=module_name,
                             message=f"[add_item]: Add new item failed, because of {e}")
                return False
        except Exception as e:
            task_id = log_params.get("task_id", "")
            system_name = log_params.get("system_name", "")
            module_name = log_params.get("module_name", "")
            logger.error(task_id=task_id, system_name=system_name, module_name=module_name,
                         message=f"[add_item]: Add new item failed, because of {e}")
            return False

    def get_all_items_not_None(self, conditions: list) -> pd.DataFrame:
        """
        获取数据库表中，字段都不为空的所有数据记录。
        """
        try:
            condition_list = [self._table.c[item] != None for item in conditions]
            select_query = select([self._table]).where(and_(*condition_list))
            result = self._engine.execute(select_query).fetchall()
            if result:
                data = pd.DataFrame(result, columns=result[0].keys())
                return data
            else:
                return pd.DataFrame()
        except (OperationalError, PyMySQLOperationalError, ConnectionResetError) as e:
            # Retry重连
            self._initialize_db()
            try:
                condition_list = [self._table.c[item] != None for item in conditions]
                select_query = select([self._table]).where(and_(*condition_list))
                result = self._engine.execute(select_query).fetchall()
                if result:
                    data = pd.DataFrame(result, columns=result[0].keys())
                    return data
                else:
                    return pd.DataFrame()
            except Exception as e:
                return pd.DataFrame()
        except Exception as e:
            return pd.DataFrame()

    def delete_item(self, identify_name: str, identify_value, log_params: dict = {}):
        """
        从数据库中删除指定的记录。

        :param item_id: 要删除记录的ID。
        :param log_params: 用于记录日志的参数字典。
        log_params = {"task_id": "aaa", "system_name": "Deviceflow", "module_name": "delete_item"}
        """
        try:
            # 创建一个删除语句，假设表中有一个"id"字段
            delete_stmt = self._table.delete().where(eval(f"self._table.c.{identify_name}") == identify_value)
            # 执行删除操作
            self._engine.execute(delete_stmt)
            return True
        except (OperationalError, PyMySQLOperationalError, ConnectionResetError) as e:
            # Retry重连
            task_id = log_params.get("task_id", "")
            logger.warning(task_id=task_id, system_name="TaskMgr", module_name="TaskTableRepo",
                           message=f"OperationalError encountered, reinitializing database connection for task_id = {task_id}")
            self._initialize_db()
            try:
                # 创建一个删除语句，假设表中有一个"id"字段
                delete_stmt = self._table.delete().where(eval(f"self._table.c.{identify_name}") == identify_value)
                # 执行删除操作
                self._engine.execute(delete_stmt)
                return True
            except Exception as e:
                print(f"[delete_item]: Delete item failed, because of {e}")
                task_id = log_params.get("task_id", "")
                system_name = log_params.get("system_name", "")
                module_name = log_params.get("module_name", "")
                logger.error(task_id=task_id, system_name=system_name, module_name=module_name,
                             message=f"[delete_item]: Delete item failed, because of {e}")
                return False
        except Exception as e:
            print(f"[delete_item]: Delete item failed, because of {e}")
            task_id = log_params.get("task_id", "")
            system_name = log_params.get("system_name", "")
            module_name = log_params.get("module_name", "")
            logger.error(task_id=task_id, system_name=system_name, module_name=module_name,
                         message=f"[delete_item]: Delete item failed, because of {e}")
            return False

if __name__ == '__main__':
    sql_table = SqlDataBase(yaml_path="ols/config/deviceflow_table.yaml")

    # get_data_by_item_values

    # set_item_value
    is_success = sql_table.set_item_value(
        identify_name = "flow_id",
        identify_value = "abc",
        item = "strategy",
        value = "sync",
        log_params = {"task_id": "aaa", "system_name": "Deviceflow", "module_name": "set_item_value"}
    )
    print(f"[set_item_value]: is_success = {is_success}")

    # get_item_value
    task_id = sql_table.get_item_value(
        identify_name="flow_id",
        identify_value="abc",
        item="task_id",
        log_params={"task_id": "aaa", "system_name": "Deviceflow", "module_name": "set_item_value"}
    )
    print(f"[get_item_value]: task_id = {task_id}")

    # check_task_in_database
    is_in_database = sql_table.check_task_in_database(
        identify_name="flow_id",
        identify_value="abc"
    )
    print(f"[check_task_in_database]: is_in_database = {is_in_database}")

    # add_item
    is_add_item = sql_table.add_item(
        item = {
            "task_id": ["bbb"],
            "task_registry": ["hello world"]
        },
        log_params={"task_id": "aaa", "system_name": "Deviceflow", "module_name": "set_item_value"}
    )
    print(f"[add_item]: is_add_item = {is_add_item}")

    # get_all_items_not_None
    result = sql_table.get_all_items_not_None(conditions=["task_id", "task_registry"])
    print(f"[get_all_items_not_None]: result = {result}")

    # delete_item
    is_delete_item = sql_table.delete_item(
        identify_name = "task_id",
        identify_value = "bbb",
        log_params={"task_id": "aaa", "system_name": "Deviceflow", "module_name": "set_item_value"}
    )
    print(f"[delete_item]: is_delete_item = {is_add_item}")

    conditions = {'task_id': "abcd", "flow_id": None}
    task_ids = sql_table.get_values_by_conditions(item="task_id", **conditions)
    print(f"task_ids = {task_ids}")