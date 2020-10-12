

```bash
eval $(minikube -p minikube docker-env)

```


```Dockerfile
FROM clojure:latest

RUN printf  '{:deps {zero.one/geni {:mvn/version "0.0.31"} \n\
                     org.apache.spark/spark-core_2.12 {:mvn/version "3.0.1" } \n\
                     org.apache.spark/spark-mllib_2.12 {:mvn/version "3.0.1"} \n\
                     org.apache.spark/spark-kubernetes_2.12 {:mvn/version  "3.0.1"}}}' >> deps.edn

RUN clj -P
CMD ["clojure"]
```

```bash
docker build -t geni .
```



```yaml

apiVersion: v1
kind: Service
metadata:
  name: headless-geni-service
  namespace spark
spec:
  clusterIP: None 
  selector:
    app: geni
  ports:
    - protocol: TCP
      port: 12345
```




```bash
kubectl run geni -ti --image=geni -n spark --serviceaccount=spark-serviceaccount --image-pull-policy="Never" --labels="app=geni"

```


```clojure
(require '[zero-one.geni.core :as g])

;; This should be the first function executed in the repl.

(g/create-spark-session
 {:app-name "my-app"
  :log-level "INFO" ;; default is WARN
  :configs
  {:spark.master "k8s://https://kubernetes.default.svc" ;;  might differ for you, its the output of kubecl cluster-info
   :spark.kubernetes.container.image "spark:v3.0.1" ;; this is for local docker images, works for minikube
   :spark.kubernetes.namespace "spark"
   :spark.kubernetes.authenticate.serviceAccountName "spark" ;; created above
   :spark.executor.instances 3
   :spark.driver.host "headless-geni-service.spark"
   :spark.driver.port 12345
   }})

```
