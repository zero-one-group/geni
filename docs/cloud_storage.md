# Cloud Storage

Spark reads and writes cloud object stores through Hadoop's filesystem connectors, so Geni's readers and writers take a cloud path as they take a local one. What changes is the classpath, which needs the store's connector, and the session, which needs the store's credentials. This guide covers Azure Data Lake Storage Gen2, a storage account with a hierarchical namespace, whose paths start with `abfss://`. Amazon S3 (`s3a://`, with Hadoop's `hadoop-aws`) and Google Cloud Storage (`gs://`, with Google's `gcs-connector`) work the same way, each with its own connector and settings.

Carsten Behring's 2020 [walk-through](https://github.com/zero-one-group/geni/issues/256) in #256 goes further, from an empty Azure account to Geni on Azure Kubernetes Service, reading a 10 GB file from Azure Files.

## The connector

Azure's connector is Hadoop's `hadoop-azure`, and its version has to match the Hadoop that Spark ships with:

| Spark | Hadoop | Dependency |
|---|---|---|
| 3.5.x | 3.3.4 | `org.apache.hadoop/hadoop-azure {:mvn/version "3.3.4"}` |
| 4.0.x | 3.4.1 | `org.apache.hadoop/hadoop-azure {:mvn/version "3.4.1"}` |
| 4.1.x | 3.4.2 | `org.apache.hadoop/hadoop-azure {:mvn/version "3.4.2"}` |
| 4.2.x | 3.5.0 | `org.apache.hadoop/hadoop-azure {:mvn/version "3.5.0"}` |

With the `:spark` setup from the README, that's an alias such as:

```edn
{:aliases
 {:azure {:extra-deps {org.apache.hadoop/hadoop-azure {:mvn/version "3.3.4"}}}}}
```

Without it, Spark fails on an `abfss://` path with `ClassNotFoundException: Class org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem not found`. Spark's own `spark-hadoop-cloud` module, at your Spark's version, brings `hadoop-azure` at the right version too, along with the S3 and Google Cloud Storage connectors and AWS's SDK, which come to hundreds of megabytes.

## Credentials

The storage account's settings go into the session's configs, each with `spark.hadoop.` in front, so that every Hadoop configuration that Spark makes has them. The examples read the account's name, a container in it and the account's key from environment variables:

<!-- #:test-doc-blocks{:meta :azure :apply :all-next} -->
```clojure
(require '[zero-one.geni.core :as g])

(def account (System/getenv "AZURE_STORAGE_ACCOUNT"))
(def container (System/getenv "AZURE_STORAGE_CONTAINER"))

(defn account-setting
  "The name of one of hadoop-azure's settings for the account."
  [setting]
  (str "spark.hadoop.fs.azure." setting "." account ".dfs.core.windows.net"))

(def spark
  (g/create-spark-session
   {:configs {(account-setting "account.key") (System/getenv "AZURE_STORAGE_KEY")}}))
```

A path names the container and the account, then the path within the container:

```clojure
(def root
  (str "abfss://" container "@" account ".dfs.core.windows.net/geni-docs"))
```

## Reading and writing

Every reader and writer takes such a path:

```clojure
(def sales
  (g/records->dataset spark [{:region "north" :amount 120}
                             {:region "south" :amount 80}]))

(g/write-parquet! sales (str root "/sales") {:mode :overwrite})

(-> (g/read-parquet! spark (str root "/sales"))
    (g/order-by :region)
    g/collect)
;; => ({:region "north", :amount 120} {:region "south", :amount 80})
```

So do `g/read!` and `g/write!`, for any format.

## A service principal

An application in Microsoft Entra ID, with a role such as Storage Blob Data Contributor on the account or the container, can sign in with its client ID and secret in place of the account's key. The settings are hadoop-azure's OAuth ones:

<!-- #:test-doc-blocks{:meta {:azure true :azure-sp true}} -->
```clojure
(def service-principal-configs
  {(account-setting "account.auth.type")
   "OAuth"
   (account-setting "account.oauth.provider.type")
   "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
   (account-setting "account.oauth2.client.id")
   (System/getenv "AZURE_CLIENT_ID")
   (account-setting "account.oauth2.client.secret")
   (System/getenv "AZURE_CLIENT_SECRET")
   (account-setting "account.oauth2.client.endpoint")
   (str "https://login.microsoftonline.com/" (System/getenv "AZURE_TENANT_ID") "/oauth2/token")
   ;; Hadoop keeps a filesystem per account for as long as the JVM runs,
   ;; with the credentials that it started with, so a session that switches
   ;; credentials in the same JVM needs it to make a new one.
   "spark.hadoop.fs.abfss.impl.disable.cache"
   "true"})

(.stop spark)

(-> (g/create-spark-session {:configs service-principal-configs})
    (g/read-parquet! (str root "/sales"))
    g/count)
;; => 2
```

## Over Spark Connect

Over Spark Connect, the server reads and writes the files, so the connector goes on the server's classpath, such as with `--packages org.apache.hadoop:hadoop-azure:3.5.0` for Spark 4.2's `sbin/start-connect-server.sh`, and the credentials go in the server's configs. The client only sends the paths.
