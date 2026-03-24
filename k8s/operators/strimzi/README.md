# Installation

```bash
helm repo add strimzi https://strimzi.io/charts/
helm repo update

helm install strimzi strimzi/strimzi-kafka-operator \
  -v 1.0.0
  -n operators \
  -f k8s/operators/strimzi/values.yaml
```