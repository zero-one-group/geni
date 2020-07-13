(defproject spark-dummy "spark-dummy"
  :dependencies [;; Core
                 [camel-snake-kebab "0.4.1"]
                 [org.clojure/clojure "1.10.1"]
                 [org.clojure/java.data "1.0.78"]
                 [potemkin "0.4.5"]
                 ;; Dev
                 [expound "0.8.5"]
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
                 [ml.dmlc/xgboost4j_2.12 "1.0.0"]])
