(defproject spark-dummy "spark-dummy"
  :dependencies [;; Core
                 [camel-snake-kebab "0.4.1"]
                 [org.clojure/clojure "1.10.1"]
                 [org.clojure/java.data "1.0.78"]
                 [potemkin "0.4.5"]
                 ;; Dev
                 [expound "0.8.4"]
                 [lein-cloverage "1.1.2"]
                 [lein-midje "3.2.1"]
                 [midje "1.9.9"]
                 ;; Spark
                 [org.apache.spark/spark-avro_2.12 "3.0.0"]
                 [org.apache.spark/spark-core_2.12 "3.0.0"]
                 [org.apache.spark/spark-hive_2.12 "3.0.0"]
                 [org.apache.spark/spark-mllib_2.12 "3.0.0"]
                 [org.apache.spark/spark-sql_2.12 "3.0.0"]
                 [org.apache.spark/spark-streaming_2.12 "3.0.0"]
                 ;; Optional: Dataproc
                 [org.apache.spark/spark-yarn_2.12 "3.0.0"]
                 ;; Optional: Spark XGBoost
                 [ml.dmlc/xgboost4j-spark_2.12 "1.0.0"]
                 [ml.dmlc/xgboost4j_2.12 "1.0.0"]
                 ;; Optional: Google Sheets Integration
                 [com.google.api-client/google-api-client "1.30.9"]
                 [com.google.apis/google-api-services-drive "v3-rev197-1.25.0"]
                 [com.google.apis/google-api-services-sheets "v4-rev612-1.25.0"]
                 [com.google.oauth-client/google-oauth-client-jetty "1.30.6"]
                 [org.apache.hadoop/hadoop-client "2.7.3"]])
