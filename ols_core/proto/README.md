### Compile .proto file and generate python files

Install grpcio-tools if not already installed.
```
 pip show grpcio-tools
 pip install grpcio-tools==1.59
```
v0.9.0
```
cd olearning-simulator\python
python -m grpc_tools.protoc -I. --python_out=. --pyi_out=. --grpc_python_out=. ols\proto\gradient_house.proto
```


v1.2.0

```
cd olearning-simulator\python
python -m grpc_tools.protoc -I. --python_out=. --pyi_out=. --grpc_python_out=. ols\proto\deviceflow.proto
python -m grpc_tools.protoc -I. --python_out=. --pyi_out=. --grpc_python_out=. ols\proto\deviceflow_registry.proto
```
