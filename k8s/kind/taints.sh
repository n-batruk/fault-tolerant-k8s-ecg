#!/bin/bash
set -euo pipefail

kubectl taint nodes \
  -l node-pool.ecg/name=kafka \
  dedicated=kafka:NoSchedule \
  --overwrite

kubectl taint nodes \
  -l node-pool.ecg/name=postgres \
  dedicated=postgres:NoSchedule \
  --overwrite

kubectl taint nodes \
  -l node-pool.ecg/name=observability \
  dedicated=observability:NoSchedule \
  --overwrite


echo "current node placement:"
kubectl get nodes \
  -l node-pool.ecg/name \
  -L node.ecg/name \
  -L node-pool.ecg/name \
  -L topology.kubernetes.io/region \
  -L topology.kubernetes.io/zone \
  -o wide


echo "current taints:"
kubectl get nodes \
  -o custom-columns='name:.metadata.name,node-name:.metadata.labels.node\.ecg/name,pool:.metadata.labels.node-pool\.ecg/name,zone:.metadata.labels.topology\.kubernetes\.io/zone,taints:.spec.taints'