# Geni on Dataproc

## Dataproc Setup

See the following guide to [setup dataproc on GCP](https://cloud.google.com/solutions/monte-carlo-methods-with-hadoop-spark) an to [create a dataproc cluster](https://cloud.google.com/dataproc/docs/guides/create-cluster#creating_a_cloud_dataproc_cluster). 


For this example, use the `preview` image version so that the cluster runs Spark 3. For instance, the following `gcloud` command creates a small dataproc cluster called `geni-cluster`:

```bash
gcloud dataproc clusters create geni-cluster \
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

## Standalone Applications

Java should already be installed on the primary node. Install Leiningen using:

```bash
wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein && \
    sudo mv lein /usr/bin/ && \
    chmod a+x /usr/bin/lein && \
    lein
```

Then, create a templated Geni app and create an uberjar:

```bash
lein new geni app && cd app
```

Delete all optional dependencies on the `project.clj` file, and match the Hadoop client installed on the dataproc primary node. At the time of writing, we would need to add the following dependency:

```clojure
[org.apache.hadoop/hadoop-client "3.2.1"]
```

and set the master to `yarn`. The Spark sesssion declaration should look like:

```clojure
(defonce spark (delay (g/create-spark-session {})))
```

Finally create an uberjar and run it on the dataproc cluster using `spark-submit`:

```bash
lein uberjar && \
    spark-submit --class app.core target/uberjar/app-0.0.1-SNAPSHOT-standalone.jar
```
