# Create kind cluster
kind create cluster --config ./k8s/kind/kind-config.yaml

# Apply taints
./k8s/kind/taints.sh

# Create namespaces
kubectl apply -R -f ./k8s/namespaces/

# Helm components installation

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana https://grafana.github.io/helm-charts
helm repo add strimzi https://strimzi.io/charts/
helm repo add cnpg https://cloudnative-pg.github.io/charts
helm repo add kedacore https://kedacore.github.io/charts

helm repo update

helm upgrade --install ecg-monitoring prometheus-community/kube-prometheus-stack \
  --version 86.2.0 \
  --namespace observability \
  --values k8s/observability/helm-values/prom-values.yaml

helm upgrade --install alloy grafana/alloy \
  --version 1.8.2 \
  --namespace observability \
  --values k8s/observability/helm-values/alloy-values.yaml

helm upgrade --install loki grafana/loki \
  --version 7.0.0 \
  --namespace observability \
  --values k8s/observability/helm-values/loki-values.yaml

helm upgrade --install strimzi strimzi/strimzi-kafka-operator \
  --version 1.0.0 \
  --namespace operators \
  --values k8s/operators/strimzi/values.yaml

helm upgrade --install cnpg cnpg/cloudnative-pg \
  --version 0.28.0 \
  --namespace operators \
  --values k8s/operators/cloudnative-pg/values.yaml

helm upgrade --install keda kedacore/keda \
  --namespace autoscaling 
```

# Create PV
```bash
kubectl apply -f ./k8s/storage/kafka-local-storage.yaml
```
# Deploy metrics server
```bash
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
```
# Apply dashboards
```bash
kubectl apply -k .\k8s\observability\dashboards\ --server-side
```
# Build and load service images
```bash
docker build -f services/grpc-stream-adapter/Dockerfile -t ecg/grpc-stream-adapter:v2 .
docker build -f services/ecg-generator/Dockerfile -t ecg/ecg-generator:v1 .
docker build -f services/ecg-storage-consumer/Dockerfile -t ecg/ecg-storage-consumer:v1 .
docker build -f services/longpoll-puller/Dockerfile -t ecg/longpoll-puller:v1 .

kind load docker-image ecg/grpc-stream-adapter:v2 --name ecg-k8s
kind load docker-image ecg/ecg-generator:v1 --name ecg-k8s
kind load docker-image ecg/ecg-storage-consumer:v1 --name ecg-k8s
kind load docker-image ecg/longpoll-puller:v1 --name ecg-k8s
```
# Apply the rest of the cluster resources (you may get some errors because kind spec will not be parsed)
```bash
kubectl apply -R -f ./k8s/
```