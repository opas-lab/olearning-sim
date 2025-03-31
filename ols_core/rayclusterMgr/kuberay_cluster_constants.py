from pydantic import BaseModel
from typing import Optional

# Group, Version, Plural
GROUP = "ray.io"
VERSION = "v1alpha1"
PLURAL = "rayclusters"
KIND = "RayCluster"

class RayCluster(BaseModel):
    k8s_namespace      : Optional[str] = "default"
    ray_name           : Optional[str] = "ray-demo1"
    ray_label          : Optional[str] = "demo1-cluster"
    head_ray_image     : Optional[str] = "rayproject/ray:2.5.0"
    head_cpu_request   : Optional[str] = "2"
    head_cpu_limit     : Optional[str] = "3"
    head_mem_request   : Optional[str] = "1G"
    head_mem_limit     : Optional[str] = "2G"
    worker_ray_image   : Optional[str] = "rayproject/ray:2.5.0"
    worker_cpu_request : Optional[str] = "2"
    worker_cpu_limit   : Optional[str] = "3"
    worker_mem_request : Optional[str] = "1G"
    worker_mem_limit   : Optional[str] = "2G"
    worker_replicas    : Optional[str] = 4
    worker_minReplicas : Optional[str] = 1
    worker_maxReplicas : Optional[str] = 6
