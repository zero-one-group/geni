(defproject spark-dummy "spark-dummy"
  :dependencies [;; Core
                 [camel-snake-kebab "0.4.2"]
                 [com.taoensso/nippy "3.1.1"]
                 [nrepl "0.8.3"]
                 [org.clojure/clojure "1.10.3"]
                 [org.clojure/java.data "1.0.86"]
                 [potemkin "0.4.5"]
                 [reply "0.5.1" :exclusions [javax.servlet/servlet-api]]
                 [zero.one/fxl "0.0.5"]
                 ;; Dev
                 [criterium "0.4.6"]
                 [enlive "1.1.6"]
                 [expound "0.8.7"]
                 [lein-ancient "0.6.15"]
                 [lein-cljfmt "0.7.0"]
                 [lein-cloverage "1.2.2"]
                 [lein-midje "3.2.1"]
                 [midje "1.9.10"]
                 [techascent/tech.ml.dataset "5.11"
                  :exclusions [ch.qos.logback/logback-classic]]
                 ;; Spark
                 [org.apache.spark/spark-avro_2.12 "3.1.1"]
                 [org.apache.spark/spark-core_2.12 "3.1.1"]
                 [org.apache.spark/spark-hive_2.12 "3.1.1"]
                 [org.apache.spark/spark-mllib_2.12 "3.1.1"]
                 [org.apache.spark/spark-sql_2.12 "3.1.1"]
                 [org.apache.spark/spark-streaming_2.12 "3.1.1"]
                 [com.github.fommil.netlib/all "1.1.2" :extension "pom"]
                 ; Arrow
                 [org.apache.arrow/arrow-memory-netty "2.0.0"]
                 [org.apache.arrow/arrow-memory-core "2.0.0"]
                 [org.apache.arrow/arrow-vector "2.0.0"
                  :exclusions [commons-codec com.fasterxml.jackson.core/jackson-databind]]
                 ; Databases
                 [mysql/mysql-connector-java "8.0.23"]
                 [org.postgresql/postgresql "42.2.19"]
                 [org.xerial/sqlite-jdbc "3.34.0"]
                 ; EDN
                 [metosin/jsonista "0.3.0"]
                 ;; Optional: Dataproc
                 [org.apache.spark/spark-yarn_2.12 "3.1.1"]
                 ;; Optional: Spark XGBoost
                 [ml.dmlc/xgboost4j-spark_2.12 "1.2.0"]
                 [ml.dmlc/xgboost4j_2.12 "1.2.0"]])
