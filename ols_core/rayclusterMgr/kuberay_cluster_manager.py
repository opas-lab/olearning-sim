from ols.rayclusterMgr import kuberay_cluster_api
from ols.rayclusterMgr import kuberay_cluster_utils, kuberay_cluster_builder
from ols.rayclusterMgr.kuberay_cluster_constants import RayCluster
from ols.proto import rayclusterService_pb2 as rayclusterService__pb2
from ols.proto.rayclusterService_pb2_grpc import RayClusterMgrServicer
import os
import configparser
from ols.simu_log import Logger

class RayClusterManager(RayClusterMgrServicer):
    def __init__(self):
        # 1). k8s init
        self._kuberay_api = kuberay_cluster_api.RayClusterApi()
        self._cr_builder = kuberay_cluster_builder.ClusterBuilder()
        self._cluster_utils = kuberay_cluster_utils.ClusterUtils()        
        # 2). ray cluster
        self._ray_cluster = RayCluster()
        # 3). log
        self._logger = Logger()
        # 4). config
        self._config = dict()
        self.getConfig()
        
    def getConfig(self):
        currentPath = os.path.abspath(os.path.dirname(__file__))
        parentPath = os.path.abspath(os.path.join(currentPath, '..'))
        # 1). set log
        logPath = parentPath + "/config/repo_log.yaml"
        if not os.path.exists(logPath):    
            raise Exception(f"logPath:{logPath} is not exist") 
        self._logger.set_logger(log_yaml = logPath)
        # 2). read config file
        configPath = os.path.abspath(os.path.join(parentPath, './config/config.conf'))
        self._config = configparser.ConfigParser()
        if not os.path.exists(configPath):
            self._logger.error(task_id = "", system_name = "RayClusterMgr", module_name = "kuberay_cluster_manager",
                message = f"[getConfig]: configPath:{configPath} is not exist.")
            raise Exception(f"configPath:{configPath} is not exist")        
        self._config.read(configPath)
        # 3). fill ray config
        self._ray_cluster.k8s_namespace      = self._config.get("ray_cluster", "k8s_namespace")
        self._ray_cluster.ray_name           = self._config.get("ray_cluster", "ray_name")
        self._ray_cluster.ray_label          = self._config.get("ray_cluster", "ray_label")
        self._ray_cluster.head_ray_image     = self._config.get("ray_cluster", "head_ray_image")
        self._ray_cluster.head_cpu_request   = self._config.get("ray_cluster", "head_cpu_request")
        self._ray_cluster.head_cpu_limit     = self._config.get("ray_cluster", "head_cpu_limit")
        self._ray_cluster.head_mem_request   = self._config.get("ray_cluster", "head_mem_request")
        self._ray_cluster.head_mem_limit     = self._config.get("ray_cluster", "head_mem_limit")
        self._ray_cluster.worker_ray_image   = self._config.get("ray_cluster", "worker_ray_image")
        self._ray_cluster.worker_cpu_request = self._config.get("ray_cluster", "worker_cpu_request")
        self._ray_cluster.worker_cpu_limit   = self._config.get("ray_cluster", "worker_cpu_limit")
        self._ray_cluster.worker_replicas    = int(self._config.get("ray_cluster", "worker_replicas"))
        self._ray_cluster.worker_minReplicas = int(self._config.get("ray_cluster", "worker_minReplicas"))
        self._ray_cluster.worker_maxReplicas = int(self._config.get("ray_cluster", "worker_maxReplicas"))

    # 1. create ray cluster, according config create ray cluster
    # no need param
    # return True or False
    def createRayCluster(self, request, context):
        try:
            # 1). init
            self._logger.info(task_id = "", system_name = "RayClusterMgr", module_name = "kuberay_cluster_manager",
                message = f"[createRayCluster]: self._ray_cluster:{self._ray_cluster}")
            cluster = ( 
                self._cr_builder.build_meta( # 输入ray群名称、名称空间、资源标签、ray版本信息
                    k8s_namespace = self._ray_cluster.k8s_namespace, 
                    name = self._ray_cluster.ray_name, 
                    labels = {self._ray_cluster.ray_label : "yes"}
                ).build_head( # ray集群head信息
                    ray_image = self._ray_cluster.head_ray_image, 
                    service_type = "ClusterIP", 
                    cpu_requests = self._ray_cluster.head_cpu_request, 
                    memory_requests = self._ray_cluster.head_mem_request
                ).build_worker( # ray集群worker信息
                    group_name = "workers", 
                    ray_image = self._ray_cluster.worker_ray_image, 
                    replicas = self._ray_cluster.worker_replicas, 
                    min_replicas = self._ray_cluster.worker_minReplicas,
                    max_replicas = self._ray_cluster.worker_maxReplicas, 
                    cpu_requests = self._ray_cluster.worker_cpu_request, 
                    memory_requests = self._ray_cluster.worker_mem_request
                ).get_cluster()
            )   
            if not self._cr_builder.succeeded:
                self._logger.error(task_id = "", system_name = "RayClusterMgr", module_name = "kuberay_cluster_manager",
                    message = f"[createRayCluster]: error building the cluster, aborting...")
                return rayclusterService__pb2.RayClusterActResponse(ok = False)
            #logfile.log.logger.info(f"[createRayCluster]: cluster:{cluster}")
            # 2). create
            result = self._kuberay_api.create_ray_cluster(body = cluster)
            if result:
                self._logger.info(task_id = "", system_name = "RayClusterMgr", module_name = "kuberay_cluster_manager",
                    message = f"[createRayCluster]: create cluster success : {result}")
                return rayclusterService__pb2.RayClusterActResponse(ok = True)
            else:
                self._logger.error(task_id = "", system_name = "RayClusterMgr", module_name = "kuberay_cluster_manager",
                    message = f"[createRayCluster]: create cluster fail!")
                return rayclusterService__pb2.RayClusterActResponse(ok = False)
        except Exception as e:
            self._logger.error(task_id = "", system_name = "RayClusterMgr", module_name = "kuberay_cluster_manager",
                message = f"[createRayCluster]: create cluster fail because of {e}")
            return rayclusterService__pb2.RayClusterActResponse(ok = False)

    # 2. modify ray cluster
    # param :
    # 1) k8s_namespace : namespace of k8s
    # 2) ray_label : unique label of ray
    # 3) worker_min_replicas : min replicase of worker
    # 4) worker_replicas : current replicase of worker
    # 5) worker_max_replicas : max replicase of worker
    # return : True or False
    def modifyRayCluster(self, request, context):
        try:
            #1). check param
            if( request.k8s_namespace == "" 
                or request.ray_name == "" 
                or request.ray_label == ""
                or request.worker_min_replicas < 0 
                or request.worker_replicas < 0 
                or request.worker_max_replicas < 0 ):
                return rayclusterService__pb2.RayClusterActResponse(ok = False)
            #2). init
            cluster = ( 
                self._cr_builder.build_meta( # 输入ray群名称、名称空间、资源标签、
                    k8s_namespace = request.k8s_namespace, 
                    name = request.ray_name, 
                    labels = {request.ray_label : "yes"}
                ).build_head( # ray集群head信息
                    ray_image = self._ray_cluster.head_ray_image, 
                    service_type = "ClusterIP", 
                    cpu_requests = self._ray_cluster.head_cpu_request, 
                    memory_requests = self._ray_cluster.head_mem_request
                ).build_worker( # ray集群worker信息
                    group_name = "workers", 
                    ray_image = self._ray_cluster.worker_ray_image,
                    cpu_requests = self._ray_cluster.worker_cpu_request, 
                    memory_requests = self._ray_cluster.worker_mem_request
                ).get_cluster())
            #3). modify
            cluster_to_patch, succeeded = self._cluster_utils.update_worker_group_replicas(
                cluster, 
                group_name = "workers", 
                max_replicas = request.worker_max_replicas, 
                min_replicas = request.worker_min_replicas, 
                replicas = request.worker_replicas)   
            if succeeded:
                # modify ray cluster
                result = self._kuberay_api.patch_ray_cluster(
                    name = cluster_to_patch["metadata"]["name"], 
                    ray_patch = cluster_to_patch)   
                if result:
                    self._logger.info(task_id = "", system_name = "RayClusterMgr", module_name = "kuberay_cluster_manager",
                        message = f"[modifyRayCluster]: modify ray cluster success!")
                    return rayclusterService__pb2.RayClusterActResponse(ok = True)
                else:
                    self._logger.error(task_id = "", system_name = "RayClusterMgr", module_name = "kuberay_cluster_manager",
                        message = f"[modifyRayCluster]:modify ray cluster fail!")
                    return rayclusterService__pb2.RayClusterActResponse(ok = False)
        except Exception as e:
            self._logger.error(task_id = "", system_name = "RayClusterMgr", module_name = "kuberay_cluster_manager",
                message = f"[modifyRayCluster]:modify ray cluster fail because of {e}")
            return rayclusterService__pb2.RayClusterActResponse(ok = False)

    # 3. delete ray cluster
    # param :
    # 1) k8s_namespace : k8s namespace  use "default" commonly
    # 2) ray_label : label of ray cluster  like : self._ray_cluster.ray_label
    # return : True or False
    def deleteRayCluster(self, request, context):
        try:
            #1). check param
            if request.k8s_namespace == "":
                return rayclusterService__pb2.RayClusterActResponse(ok = False)
            #2). serch ray cluster by param
            if request.ray_label == "":
                kube_ray_list = self._kuberay_api.list_ray_clusters(k8s_namespace = request.k8s_namespace)
            else:
                kube_ray_list = self._kuberay_api.list_ray_clusters(k8s_namespace = request.k8s_namespace, 
                        label_selector = f'{request.ray_label}=yes')
            #3). delete
            if "items" in kube_ray_list:
                for cluster in kube_ray_list["items"]:
                    self._logger.info(task_id = "", system_name = "RayClusterMgr", module_name = "kuberay_cluster_manager",
                        message = f'[deleteRayCluster]: deleting raycluster = \
                        {cluster["metadata"]["name"]}')
                    # 通过指定名称删除ray集群
                    self._kuberay_api.delete_ray_cluster(
                        name = cluster["metadata"]["name"],
                        k8s_namespace = cluster["metadata"]["namespace"])
            return rayclusterService__pb2.RayClusterActResponse(ok = True)
        except Exception as e:
            self._logger.error(task_id = "", system_name = "RayClusterMgr", module_name = "kuberay_cluster_manager",
                message = f"[deleteRayCluster]:delete ray cluster fail because of {e}")
            return rayclusterService__pb2.RayClusterActResponse(ok = False)

    # 4. query ray cluster
    # param :
    # 1) k8s_namespace : k8s namespace  use "default" commonly
    # 2) ray_label : label of ray cluster  like : self._ray_cluster.ray_label
    # return : json data of ray cluster    
    def queryRayCluster(self, request, context):
        try:
            #1). check param (namespace , label)
            if request.k8s_namespace == "":
                return rayclusterService__pb2.RayClusterQueryResult(json_data = "ray list: nil")
            #2). query 
            if request.ray_label == "":
                kube_ray_list = self._kuberay_api.list_ray_clusters(k8s_namespace = request.k8s_namespace)
            else:
                kube_ray_list = self._kuberay_api.list_ray_clusters(k8s_namespace = request.k8s_namespace, 
                        label_selector = f'{request.ray_label}=yes')
            #3). print
            if len(kube_ray_list.get("items")) == 0:
                return rayclusterService__pb2.RayClusterQueryResult(json_data = "ray list: nil")
            if "items" in kube_ray_list:
                for cluster in kube_ray_list["items"]:
                    self._logger.info(task_id = "", system_name = "RayClusterMgr", module_name = "kuberay_cluster_manager",
                        message = f'[queryRayCluster]: {cluster["metadata"]["name"]}  \
                        {cluster["metadata"]["namespace"]}')
            return rayclusterService__pb2.RayClusterQueryResult(json_data = 
                    f"ray list: {kube_ray_list.get('items')}")
        except Exception as e:
            self._logger.error(task_id = "", system_name = "RayClusterMgr", module_name = "kuberay_cluster_manager",
                message = f"[queryRayCluster]: query ray cluster fail because of {e}")
            rayclusterService__pb2.RayClusterQueryResult(json_data = "ray list: nil")
