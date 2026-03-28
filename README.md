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
```

# Create PV
kubectl apply -f ./k8s/storage/kafka-local-storage.yaml
