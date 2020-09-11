(defproject spark-dummy "spark-dummy"
  :dependencies [;; Core
                 [camel-snake-kebab "0.4.1"]
                 [nrepl "0.8.1"]
                 [org.clojure/clojure "1.10.1"]
                 [org.clojure/java.data "1.0.86"]
                 [potemkin "0.4.5"]
                 [reply "0.4.4"]
                 ;; Dev
                 [expound "0.8.5"]
                 [lein-ancient "0.6.15"]
                 [lein-cloverage "1.1.2"]
                 [lein-midje "3.2.1"]
                 [midje "1.9.9"]
                 ;; Spark
                 [org.apache.spark/spark-avro_2.12 "3.0.1"]
                 [org.apache.spark/spark-core_2.12 "3.0.1"]
                 [org.apache.spark/spark-hive_2.12 "3.0.1"]
                 [org.apache.spark/spark-mllib_2.12 "3.0.1"]
                 [org.apache.spark/spark-sql_2.12 "3.0.1"]
                 [org.apache.spark/spark-streaming_2.12 "3.0.1"]
                 [com.github.fommil.netlib/all "1.1.2" :extension "pom"]
                 ; Databases
                 [mysql/mysql-connector-java "8.0.21"]
                 [org.postgresql/postgresql "42.2.14"]
                 [org.xerial/sqlite-jdbc "3.32.3.1"]
                 ; EDN
                 [metosin/jsonista "0.2.7"]
                 ;; Optional: Dataproc
                 [org.apache.spark/spark-yarn_2.12 "3.0.1"]
                 ;; Optional: Spark XGBoost
                 [ml.dmlc/xgboost4j-spark_2.12 "1.0.0"]
                 [ml.dmlc/xgboost4j_2.12 "1.0.0"]])
