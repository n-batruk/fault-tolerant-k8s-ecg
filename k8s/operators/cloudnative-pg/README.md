# Installation

```bash
helm repo add cnpg https://cloudnative-pg.github.io/charts

helm upgrade --install cnpg cnpg/cloudnative-pg \
  --version 0.28.0 \
  -n operators \
  -f k8s/operators/cloudnative-pg/values.yaml
```