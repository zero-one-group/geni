
# Create Azure Kubernetes cluster and Azure Contaier Registry

```bash
az group create --name geni-azure-demo --location westeurope
az acr create --resource-group geni-azure-demo --name genidemo18w --sku Basic


az aks create --resource-group geni-azure-demo --name geniCluster --node-count 3  --generate-ssh-keys --attach-acr genidemo18w
az aks get-credentials --resource-group geni-azure-demo --name geniCluster

```

# Create storage accont

```
# Change these four parameters as needed for your own environment
AKS_PERS_STORAGE_ACCOUNT_NAME=genistorageaccount$RANDOM
AKS_PERS_RESOURCE_GROUP=geni-azure-demo
AKS_PERS_LOCATION=westeurope
AKS_PERS_SHARE_NAME=aksshare


# Create a storage account
az storage account create -n $AKS_PERS_STORAGE_ACCOUNT_NAME -g $AKS_PERS_RESOURCE_GROUP -l $AKS_PERS_LOCATION --sku Standard_LRS

# Export the connection string as an environment variable, this is used when creating the Azure file share
export AZURE_STORAGE_CONNECTION_STRING=$(az storage account show-connection-string -n $AKS_PERS_STORAGE_ACCOUNT_NAME -g $AKS_PERS_RESOURCE_GROUP -o tsv)

# Create the file share
az storage share create -n $AKS_PERS_SHARE_NAME --connection-string $AZURE_STORAGE_CONNECTION_STRING

# Get storage account key
STORAGE_KEY=$(az storage account keys list --resource-group $AKS_PERS_RESOURCE_GROUP --account-name $AKS_PERS_STORAGE_ACCOUNT_NAME --query "[0].value" -o tsv)

# Echo storage account name and key
echo Storage account name: $AKS_PERS_STORAGE_ACCOUNT_NAME
echo Storage account key: $STORAGE_KEY

```

```
kubectl create secret generic azure-secret --from-literal=azurestorageaccountname=$AKS_PERS_STORAGE_ACCOUNT_NAME --from-literal=azurestorageaccountkey=$STORAGE_KEY -n spark
```


```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: azurefile
  namespace: spark
spec:
  capacity:
    storage: 5Gi
  accessModes:
    - ReadWriteMany
  storageClassName: azurefile
  azureFile:
    secretName: azure-secret
    shareName: aksshare
    readOnly: false
```

```
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
      storage: 5Gi
```

    


# Create Docker image for driver

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
docker build -t genidemo18w.azurecr.io/geni .

az acr login --resource-group geni-azure-demo --name genidemo18w
docker push genidemo18w.azurecr.io/geni
```

# create namespaces
```bash
kubectl create namespace spark
kubectl create serviceaccount spark-serviceaccount --namespace spark
kubectl create clusterrolebinding spark-rolebinding --clusterrole=edit --serviceaccount=spark:spark-serviceaccount --namespace=spark

```

# Create headless service

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
kubectl apply -f headless.yaml
```


```bash
kubectl run geni -ti --image=genidemo18w.azurecr.io/geni -n spark --serviceaccount=spark-serviceaccount --labels="app=geni"

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
