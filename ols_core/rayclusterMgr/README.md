rayclusterMgr前置要求：

1). 安装 KubeRay： ! helm install kuberay-operator kuberay/kuberay-operator --version 0.5.0
2). 安装 k8s sdk: pip install kubernetes
3). 将python_client拷贝到PYTHONPATH路径下或者直接安装python_client, 该库路径为:https://github.com/ray-project/kuberay/tree/master/clients/python-client/python_client
