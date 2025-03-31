import os
import yaml
import logging
from ols.ofl_commons.infrastructure.FileRepo.s3_file_repo import S3FileRepo
from ols.ofl_commons.infrastructure.TaskRepo.sql_task_repo import SqlTaskRepo

import pulsar
import pathlib

def getSqlTaskRepo(repo_config=""):
    with open(repo_config, 'r') as file:
        repo_config = yaml.load(file, Loader=yaml.FullLoader)
    # Create MySql task repo.
    sql_task_repo = SqlTaskRepo(
        host=repo_config['host'],
        user=repo_config['user'],
        password=repo_config['password'],
        port=repo_config['port'],
        database=repo_config['database']
    )
    return sql_task_repo


def getPulsarObjects(manager_config=""):
    with open(manager_config, 'r') as file:
        manager_config = yaml.load(file, Loader=yaml.FullLoader)
    pulsar_client = pulsar.Client(manager_config['service_url'])
    pulsar_producer = pulsar_client.create_producer(manager_config['topic'][0])
    return pulsar_client, pulsar_producer

def get_gradient_house_pulsar(config=""):
    with open(config, 'r') as file:
        gradient_house_config = yaml.load(file, Loader=yaml.FullLoader)
    pulsar_client = pulsar.Client(gradient_house_config['inbound_pulsar_url'])
    pulsar_producer = pulsar_client.create_producer(gradient_house_config['inbound_topic_url'])
    return pulsar_client, pulsar_producer


def getS3FileRepo(manager_config=""):
    with open(manager_config, 'r') as file:
        manager_config = yaml.load(file, Loader=yaml.FullLoader)
    s3_file_repo = S3FileRepo(manager_config)
    return s3_file_repo


def getS3SelectionFiles(
    manager_config = "",
    jsonfiles_path = ""
):
    '''
    获得经过筛选服务后的配置, 返回所有配置的名称列表
    '''
    import fnmatch

    with open(manager_config, 'r') as file:
        manager_config = yaml.load(file, Loader=yaml.FullLoader)
    s3_file_repo = S3FileRepo(manager_config)

    # 遍历获得s3的所有文件路径
    objects = []
    continuation_token = ""
    while True:
        if continuation_token == "":
            list_kwargs = {
                'Bucket': s3_file_repo._bucket_id
            }
        else:
            list_kwargs = {
                'Bucket': s3_file_repo._bucket_id,
                'ContinuationToken': continuation_token
            }
        response = s3_file_repo.client.list_objects_v2(**list_kwargs)
        objects.extend(response['Contents'])
        if 'NextContinuationToken' in response:
            continuation_token = response['NextContinuationToken']
        else:
            break

    selection_jsonfiles = []
    for obj in objects:
        if fnmatch.fnmatch(obj['Key'], f"{jsonfiles_path}/*.json"):
            selection_jsonfiles.append(obj['Key'])
    return selection_jsonfiles


def downloadS3File(
    manager_config = "",
    local_file = "",
    taget_file = "",
):
    '''
    从S3处下载对应文件
    '''
    # try:
    with open(manager_config, 'r') as file:
        manager_config = yaml.load(file, Loader=yaml.FullLoader)
    s3_file_repo = S3FileRepo(manager_config)

    file_obj = pathlib.Path(local_file)
    if file_obj.exists(): #如果存在本地文件
        logging.info(f"{local_file} exists!")
        return True
    else:
        return s3_file_repo.download_file(
            local_path = local_file,
            target_path = taget_file
        )


def deleteS3File(manager_config = "", taget_file = "") -> bool:
    with open(manager_config, 'r') as file:
        manager_config = yaml.load(file, Loader=yaml.FullLoader)
    s3_file_repo = S3FileRepo(manager_config)
    return s3_file_repo.delete_file(target_path=taget_file)