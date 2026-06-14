Перед початком розгортання необхідно мати локальну копію репозиторію з Kubernetes-маніфестами та вихідним кодом сервісів. Подальші команди виконуються з кореневої директорії проєкту.

Перед створенням кластера потрібно перевірити наявність основних інструментів:

```bash
docker --version
kubectl version --client
kind version
helm version
```

Також необхідно переконатися, що Docker запущений і доступний поточному користувачу:

```bash
docker info
```

На першому етапі створюється локальний Kubernetes-кластер за допомогою kind:

```bash
kind create cluster --config ./k8s/kind/config.yaml
```

Після створення кластера перевіряється доступність вузлів:

```bash
kubectl get nodes -o wide
```

Далі для окремих вузлів додаються taints, які використовуються для розміщення компонентів відповідно до їх ролі в демонстраційному середовищі. Перед запуском скрипта потрібно надати йому право на виконання:

```bash
chmod +x ./k8s/kind/taints.sh
./k8s/kind/taints.sh
```

Після цього можна повторно перевірити стан вузлів:

```bash
kubectl get nodes -o wide
```

На наступному етапі створюються namespace-и, у яких розміщуються окремі частини системи:

```bash
kubectl apply -R -f ./k8s/namespaces/
```

Після застосування маніфестів перевіряється наявність створених namespace-ів:

```bash
kubectl get namespaces
```

Після підготовки namespace-ів додаються Helm-репозиторії, з яких встановлюються оператори та допоміжні компоненти системи:

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana https://grafana.github.io/helm-charts
helm repo add strimzi https://strimzi.io/charts/
helm repo add cnpg https://cloudnative-pg.github.io/charts
helm repo add kedacore https://kedacore.github.io/charts

helm repo update
```

Після оновлення Helm-репозиторіїв встановлюються компоненти системи спостережуваності, оператор Strimzi для керування Kafka-кластером, оператор CloudNativePG для керування PostgreSQL-кластером та KEDA для автоматичного масштабування.

Встановлення kube-prometheus-stack:

```bash
helm upgrade --install ecg-monitoring prometheus-community/kube-prometheus-stack \
  --version 86.2.0 \
  --namespace observability \
  --values k8s/observability/helm-values/prom-values.yaml
```

Встановлення Grafana Alloy:

```bash
helm upgrade --install alloy grafana/alloy \
  --version 1.8.2 \
  --namespace observability \
  --values k8s/observability/helm-values/alloy-values.yaml
```

Встановлення Grafana Loki:

```bash
helm upgrade --install loki grafana/loki \
  --version 7.0.0 \
  --namespace observability \
  --values k8s/observability/helm-values/loki-values.yaml
```

Встановлення Strimzi operator:

```bash
helm upgrade --install strimzi strimzi/strimzi-kafka-operator \
  --version 1.0.0 \
  --namespace operators \
  --values k8s/operators/strimzi/values.yaml
```

Встановлення CloudNativePG operator:

```bash
helm upgrade --install cnpg cnpg/cloudnative-pg \
  --version 0.28.0 \
  --namespace operators \
  --values k8s/operators/cloudnative-pg/values.yaml
```

Встановлення KEDA:

```bash
helm upgrade --install keda kedacore/keda \
  --namespace autoscaling
```

Після встановлення операторів перевіряється стан pod-ів:

```bash
kubectl get pods -n observability
kubectl get pods -n operators
kubectl get pods -n autoscaling
```

Далі застосовуються Kubernetes-маніфести Kafka. Спочатку створюється локальне сховище для Kafka, після чого розгортаються Kafka-кластер, KafkaNodePool, теми та користувачі:

```bash
kubectl apply -f ./k8s/storage/kafka-local-storage.yaml
kubectl apply -f ./k8s/kafka/
```

Стан Kafka-компонентів перевіряється командою:

```bash
kubectl get pods -n kafka
kubectl get kafka -n kafka
kubectl get kafkatopic -n kafka
kubectl get kafkauser -n kafka
```

Після запуску Kafka розгортається PostgreSQL-кластер під керуванням CloudNativePG:

```bash
kubectl apply -f ./k8s/postgres/
```

Стан PostgreSQL-кластера перевіряється командою:

```bash
kubectl get pods -n database
kubectl get cluster -n database
```

Після запуску PostgreSQL необхідно застосувати SQL-схему бази даних. Для цього використовується тимчасовий pod з образом PostgreSQL. Пароль користувача бази даних зчитується з Kubernetes Secret.

```bash
export PG_PASSWORD="$(kubectl get secret ecg-app-user -n database -o jsonpath='{.data.password}' | base64 -d)"
```

Після цього SQL-схема застосовується командою:

```bash
cat ./db/schema.sql | kubectl run psql-init \
  -n ecg-system \
  --rm \
  -i \
  --restart=Never \
  --image=postgres:16 \
  --env="PGPASSWORD=${PG_PASSWORD}" \
  -- psql \
  -h ecg-postgres-pooler-rw.database.svc.cluster.local \
  -U ecg_app \
  -d ecg \
  -f -
```

Після завершення команди тимчасовий pod буде автоматично видалений.

Далі збираються Docker-образи прикладних сервісів. Для демонстраційного запуску використовуються локальні образи, які завантажуються безпосередньо в kind-кластер.

```bash
docker build -f services/grpc-stream-adapter/Dockerfile -t ecg/grpc-stream-adapter:v2 .
docker build -f services/ecg-generator/Dockerfile -t ecg/ecg-generator:v1 .
docker build -f services/ecg-storage-consumer/Dockerfile -t ecg/ecg-storage-consumer:v1 .
docker build -f services/longpoll-puller/Dockerfile -t ecg/longpoll-puller:v1 .
docker build -f services/mock-external-buffer-server/Dockerfile -t ecg/mock-external-buffer-server:v1 .
```

Після збірки образи завантажуються в kind-кластер:

```bash
kind load docker-image ecg/grpc-stream-adapter:v2 --name ecg-k8s
kind load docker-image ecg/ecg-generator:v1 --name ecg-k8s
kind load docker-image ecg/ecg-storage-consumer:v1 --name ecg-k8s
kind load docker-image ecg/longpoll-puller:v1 --name ecg-k8s
kind load docker-image ecg/mock-external-buffer-server:v1 --name ecg-k8s
```

Після завантаження Docker-образів у kind-кластер розгортаються прикладні компоненти системи:

```bash
kubectl apply -R -f ./k8s/apps/
```

Стан прикладних pod-ів перевіряється командою:

```bash
kubectl get pods -n ecg-system
```

Для сценарію тривалого опитування додатково запускається зовнішній mock buffer server. У демонстраційному середовищі він може бути запущений як окремий Docker-контейнер на хості:

```bash
docker run -d \
  --name ecg-mock-buffer-server \
  -p 18080:8080 \
  -e HTTP_HOST=0.0.0.0 \
  -e HTTP_PORT=8080 \
  -e SOURCE_ID=mock-external-buffer-server-001 \
  -e SESSION_ID=longpoll-session-001 \
  -e SAMPLING_RATE=500 \
  -e LEAD_ID=I \
  -e CHUNK_DURATION_SECONDS=1 \
  -e GENERATION_INTERVAL_SECONDS=1.0 \
  -e BUFFER_CHUNKS=100 \
  -e PRELOAD_CHUNKS=30 \
  -e LOG_LEVEL=INFO \
  ecg/mock-external-buffer-server:v1
```

Перевірити запуск контейнера можна командою:

```bash
docker ps --filter name=ecg-mock-buffer-server
```

Після запуску всіх компонентів виконується тестове передавання ЕКГ-фрагментів. Для цього запускається Kubernetes Job генератора:

```bash
kubectl apply -f ./k8s/apps/ecg-generator/job.yaml
```

Стан Job перевіряється командою:

```bash
kubectl get jobs -n ecg-system
kubectl get pods -n ecg-system
```

Логи генератора можна переглянути командою:

```bash
kubectl logs -n ecg-system job/ecg-generator
```

У разі коректної роботи генератор має завершитися з повідомленням про успішне приймання всіх фрагментів.

Після запуску основних компонентів завантажуються дашборди для моніторингу CloudNativePG та Strimzi:

```bash
kubectl apply -k ./k8s/observability/dashboards/ --server-side
```

Для доступу до Grafana використовується port-forward:

```bash
kubectl port-forward -n observability svc/ecg-monitoring-grafana 3000:80
```

Після цього Grafana доступна в браузері за адресою:

```text
http://localhost:3000
```

Після розгортання всіх компонентів можна виконати загальну перевірку стану системи:

```bash
kubectl get pods -A
kubectl get svc -A
kubectl get pvc -A
kubectl get pv
```

Окремо можна перевірити стан Kafka, PostgreSQL та прикладних компонентів:

```bash
kubectl get kafka -n kafka
kubectl get kafkatopic -n kafka
kubectl get cluster -n database
kubectl get pods -n ecg-system
```

Після завершення демонстрації можна зупинити зовнішній mock buffer server:

```bash
docker stop ecg-mock-buffer-server
docker rm ecg-mock-buffer-server
```

Для повного видалення локального kind-кластера використовується команда:

```bash
kind delete cluster --name ecg-k8s
```
