# Geni on Dataproc

## Dataproc Setup

See the following guide to [setup dataproc on GCP](https://cloud.google.com/solutions/monte-carlo-methods-with-hadoop-spark) an to [create a dataproc cluster](https://cloud.google.com/dataproc/docs/guides/create-cluster#creating_a_cloud_dataproc_cluster). We will be using [Google Cloud SDK](https://cloud.google.com/sdk/install) with the `gcloud` CLI commands.

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

This could take a few minutes to run. Then access the primary node using:

```bash
gcloud compute ssh ubuntu@geni-cluster-m
```

## Running Geni on Yarn

Java should already be installed on the primary node. Install Leiningen using:

```bash
wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein && \
    sudo mv lein /usr/bin/ && \
    chmod a+x /usr/bin/lein && \
    lein
```

Then, create a templated Geni app and step into the app directory::

```bash
lein new geni app +dataproc && cd app
```

To spawn the Spark REPL, run:

```bash
lein spark-submit
```

This is a shortcut to creating an uberjar and running it using `spark-submit`. By default, the templated main function:

1. prints the Spark configuration;
2. runs a Spark ML example;
3. starts an nREPL server on port 65204; and
4. steps into a [REPL(-y)](https://github.com/trptcolin/reply).

Verify that `spark.master` is set to `"yarn"`. To submit a standalone application, we can simply edit the `-main` function on `core.clj`. Remove the `launch-repl` function to prevent stepping into the REPL. 

## Cleaning Up

Once finished with the exercise, the easiest way to clean up is to simply delete the GCP project.

Alternatively, delete the cluster using:

```bash
gcloud dataproc clusters delete geni-cluster --region=asia-southeast1
```

There may be dangling storage buckets that have to be deleted separately.
