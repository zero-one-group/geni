# Geni on Dataproc

## Dataproc Setup

See the following guide to [setup dataproc on GCP](https://cloud.google.com/solutions/monte-carlo-methods-with-hadoop-spark) an to [create a dataproc cluster](https://cloud.google.com/dataproc/docs/guides/create-cluster#creating_a_cloud_dataproc_cluster). 


For this example, use the `preview` image version so that the cluster runs Spark 3. For instance, the following `gcloud` command creates a small dataproc cluster called `geni-cluster`:

```bash
gcloud dataproc clusters create geni-cluster \
    --region=asia-southeast1 \
    --master-machine-type n1-standard-1 \
    --master-boot-disk-size 30 \
    --num-workers 2 \
    --worker-machine-type n1-standard-1 \
    --worker-boot-disk-size 30 \
    --image-version=preview
```

Then access the primary node using:

```bash
gce geni-cluster-m ssh
```

Once finished with the exercise, delete the cluster using:

```
gcloud dataproc clusters delete geni-cluster --region=asia-southeast1
```

There may be dangling storage buckets that have to be deleted separately.

## Standalone Applications

Java should already be installed on the primary node. Install Leiningen using:

```bash
wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein && \
    sudo mv lein /usr/bin/ && \
    chmod a+x /usr/bin/lein && \
    lein
```

Then, create a templated Geni app and step into the directory:

```bash
lein new geni app && cd app
```

Delete all optional dependencies on the `project.clj` file, namely:

```clojure
;; Optional: Spark XGBoost
[ml.dmlc/xgboost4j-spark_2.12 "1.0.0"]
[ml.dmlc/xgboost4j_2.12 "1.0.0"]
;; Optional: Google Sheets Integration
[com.google.api-client/google-api-client "1.30.9"]
[com.google.apis/google-api-services-drive "v3-rev197-1.25.0"]
[com.google.apis/google-api-services-sheets "v4-rev612-1.25.0"]
[com.google.oauth-client/google-oauth-client-jetty "1.30.6"]
[org.apache.hadoop/hadoop-client "2.7.3"]
```

Match the Hadoop client installed on the dataproc primary node. At the time of writing, we would need to add the following dependency:

```clojure
[org.apache.hadoop/hadoop-client "3.2.1"]
```

Set the master to `yarn`. For instance, the Spark sesssion definition may look like:

```clojure
(defonce spark (delay (g/create-spark-session {:master "yarn"})))
```

Finally create an uberjar and run it on the dataproc cluster using `spark-submit`:

```bash
lein uberjar && \
    spark-submit --class app.core target/uberjar/app-0.0.1-SNAPSHOT-standalone.jar
```

Once the uberjar ran the default script successfully, we can jump back to and edit `core.clj` to run our own script.
