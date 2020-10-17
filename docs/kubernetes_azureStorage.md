
# Prerequisits

* az cli 
* docker
* kubectl
* access to azure subscription with Contributor or Owner role


# Create resource group in azure

```bash
az group create --name geni-azure-demo --location westeurope
```

# Create Azure Container Registry 

```bash
az acr create --resource-group geni-azure-demo --name genidemo18w --sku Basic
```
# Create Kubernetes Cluster

```bash
az aks create --resource-group geni-azure-demo --name geniCluster --node-count 3  --generate-ssh-keys --attach-acr genidemo18w
```
# install Kubernetes credentials into kubectl

```bash
az aks get-credentials --resource-group geni-azure-demo --name geniCluster
```

# Create storage account to hold data to analyse

```
# Change these four parameters as needed for your own environment
export AKS_PERS_STORAGE_ACCOUNT_NAME=genistorageaccount$RANDOM
export AKS_PERS_RESOURCE_GROUP=geni-azure-demo
export AKS_PERS_LOCATION=westeurope
export AKS_PERS_SHARE_NAME=aksshare


# Create a storage account
az storage account create -n $AKS_PERS_STORAGE_ACCOUNT_NAME -g $AKS_PERS_RESOURCE_GROUP -l $AKS_PERS_LOCATION --sku Standard_LRS

# Export the connection string as an environment variable, this is used when creating the Azure file share
export AZURE_STORAGE_CONNECTION_STRING=$(az storage account show-connection-string -n $AKS_PERS_STORAGE_ACCOUNT_NAME -g $AKS_PERS_RESOURCE_GROUP -o tsv)

# Create the file share
az storage share create -n $AKS_PERS_SHARE_NAME --connection-string $AZURE_STORAGE_CONNECTION_STRING

# Get storage account key
export STORAGE_KEY=$(az storage account keys list --resource-group $AKS_PERS_RESOURCE_GROUP --account-name $AKS_PERS_STORAGE_ACCOUNT_NAME --query "[0].value" -o tsv)

# Echo storage account name and key
echo Storage account name: $AKS_PERS_STORAGE_ACCOUNT_NAME
echo Storage account key: $STORAGE_KEY

```
# Create persistent volume claim in Kubernetes
## create namespace and service account in kubernetes

```bash
kubectl create namespace spark
kubectl create serviceaccount spark-serviceaccount --namespace spark
kubectl create clusterrolebinding spark-rolebinding --clusterrole=edit --serviceaccount=spark:spark-serviceaccount --namespace=spark

```

## Create secret to access storage
```
kubectl create secret generic azure-secret --from-literal=azurestorageaccountname=$AKS_PERS_STORAGE_ACCOUNT_NAME --from-literal=azurestorageaccountkey=$STORAGE_KEY -n spark
```

Create file pvc.yaml with content:

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: azurefile
  namespace: spark
spec:
  capacity:
    storage: 50Gi
  accessModes:
    - ReadWriteMany
  storageClassName: azurefile
  azureFile:
    secretName: azure-secret
    shareName: aksshare
    readOnly: false
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: azurefile
  namespace: spark
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: azurefile
  resources:
    requests:
      storage: 50Gi
```

```bash
kubectl create -f pvc.yaml
```


# Prepare Spark driver pod 
## Create Docker image for driver

```Dockerfile
FROM clojure:latest
RUN apt-get update && apt-get install -y wget
RUN printf  '{:deps {zero.one/geni {:mvn/version "0.0.31"} \n\
                     org.apache.spark/spark-core_2.12 {:mvn/version "3.0.1" } \n\
                     org.apache.spark/spark-mllib_2.12 {:mvn/version "3.0.1"} \n\
                     org.apache.spark/spark-kubernetes_2.12 {:mvn/version  "3.0.1"}}}' >> deps.edn

RUN clj -P
CMD ["clojure"]
```
## build driver image

```bash
docker build -t genidemo18w.azurecr.io/geni .
```
Push image to registry

```bash
az acr login --resource-group geni-azure-demo --name genidemo18w
docker push genidemo18w.azurecr.io/geni
```




## Create headless service for Spark driver

write to headless.yaml
```yaml

apiVersion: v1
kind: Service
metadata:
  name: headless-geni-service
  namespace: spark
spec:
  clusterIP: None 
  selector:
    app: geni
  ports:
    - protocol: TCP
      port: 12345
```

```bash
kubectl create -f headless.yaml
```

## Start Spark driver pod

write into driver.yaml

```yaml
apiVersion: v1
kind: Pod
metadata:
  namespace: spark
  name: geni
  labels:
    app: geni
spec:
  volumes:
    - name: data-storage
      persistentVolumeClaim:
        claimName: azurefile
  containers:
  - name: geni
    image: genidemo18w.azurecr.io/geni
    command: [ "/bin/bash", "-c", "--" ]
    args: [ "while true; do sleep 30; done;" ]
    volumeMounts:
        - mountPath: "/data"
          name: data-storage
  serviceAccountName: spark-serviceaccount
  restartPolicy: Never
```

```bash
kubectl create -f driver.yaml
```


# Prepare Spark worker pods

## Install spark distribution
Running spark on Kubernetes requires to create Docker images for the nodes.

The Spark distribution contains tools to ease the creation of suitable Docker images,
so we need to download it first.

```bash
wget https://downloads.apache.org/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz
tar xzf spark-3.0.1-bin-hadoop2.7.tgz
cd spark-3.0.1-bin-hadoop2.7/
```

## Download  additional jars into spark distribution
needed ?
```bash
#cd jars
#wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/2.7.4/hadoop-azure-2.7.4.jar
#wget https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/2.0.0/azure-storage-2.0.0.jar
```

## Build images for workers

```bash

bin/docker-image-tool.sh -r genidemo18w.azurecr.io -t v3.0.1 build
```

## Push images for worker into registry

```bash
bin/docker-image-tool.sh -r genidemo18w.azurecr.io -t v3.0.1 push
```

# Copy data into storage

```bash
kubectl exec -ti geni -n spark -- /bin/bash
```

https://data.cityofnewyork.us/Transportation/2018-Yellow-Taxi-Trip-Data/t29m-gskq

```bash
cd /data
# its 10 GB, takes a while
wget "https://data.cityofnewyork.us/api/views/t29m-gskq/rows.csv?accessType=DOWNLOAD" -O nyc_taxi.csv
```




# Run clojre/geni code on driver pod
## run Clojure repl on existing pod

```bash
kubectl exec -tui geni -n spark -- clj
```


```clojure
(require '[zero-one.geni.core :as g])

;; This should be the first function executed in the repl.

(g/create-spark-session
 {:app-name "my-app"
  :log-level "INFO" ;; default is WARN
  :configs
  {:spark.master "k8s://https://kubernetes.default.svc" ;;  might differ for you, its the output of kubecl cluster-info
   :spark.kubernetes.container.image "genidemo18w.azurecr.io/spark:v3.0.1" ;; this is for local docker images, works for minikube
   :spark.kubernetes.namespace "spark"
   :spark.kubernetes.authenticate.serviceAccountName "spark-serviceaccount" ;; created above
   :spark.executor.instances 3
   :spark.driver.host "headless-geni-service.spark"
   :spark.driver.port 12345
   :spark.kubernetes.executor.volumes.persistentVolumeClaim.azurefile.mount.path  "/data"
   :spark.kubernetes.executor.volumes.persistentVolumeClaim.azurefile.options.claimName "azurefile"
   }})

```
# access Spark web ui
In an other bash shell

```bash
kubectl port-forward pod/geni -n spark 4040:4040
```

## start analysis

```clojure
(def df (g/read-csv! "/data/nyc_taxi.csv" {:inferSchema false :kebab-columns true}))
(g/print-schema df)
root
 |-- vendor-id: string (nullable = true)
 |-- tpep-pickup-datetime: string (nullable = true)
 |-- tpep-dropoff-datetime: string (nullable = true)
 |-- passenger-count: string (nullable = true)
 |-- trip-distance: string (nullable = true)
 |-- ratecode-id: string (nullable = true)
 |-- store-and-fwd-flag: string (nullable = true)
 |-- pu-location-id: string (nullable = true)
 |-- do-location-id: string (nullable = true)
 |-- payment-type: string (nullable = true)
 |-- fare-amount: string (nullable = true)
 |-- extra: string (nullable = true)
 |-- mta-tax: string (nullable = true)
 |-- tip-amount: string (nullable = true)
 |-- tolls-amount: string (nullable = true)
 |-- improvement-surcharge: string (nullable = true)
 |-- total-amount: string (nullable = true)

 ```


```clojure

(def df (g/read-csv! "/data/nyc_taxi.csv" 
  {:kebab-columns true
   :schema (g/struct-type 
    (g/struct-field :amount :long true)
    (g/struct-field :tip-amount :long true)
    (g/struct-field :tools-amount :long true)
    (g/struct-field :payment-type :string true))

    }))

(g/cache df)
(-> df  (g/agg df (g/sum  :amount)) g/show)
(-> df (g/group-by :payment-type ) (g/sum  :amount) g/show)

```


