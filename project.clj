(defproject zero.one/geni "0.0.6"
  :jvm-opts ["-Duser.country=US" "-Duser.language=en"]
  :description "A Clojure library that wraps Apache Spark"
  :url "https://github.com/zero-one-group/geni"
  :license {:name "Apache License"
            :url  "https://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[camel-snake-kebab "0.4.1"]
                 [org.clojure/clojure "1.10.1"]
                 [org.clojure/java.data "1.0.64"]
                 [potemkin "0.4.5"]]
  :profiles {:provided
             {:dependencies [[ml.dmlc/xgboost4j-spark_2.12 "1.0.0"]
                             [ml.dmlc/xgboost4j_2.12 "1.0.0"]
                             [org.apache.spark/spark-core_2.12 "2.4.5"]
                             [org.apache.spark/spark-hive_2.12 "2.4.5"]
                             [org.apache.spark/spark-mllib_2.12 "2.4.5"]
                             [org.apache.spark/spark-sql_2.12 "2.4.5"]
                             [org.apache.spark/spark-streaming_2.12 "2.4.5"]]}
             :dev {:dependencies [[expound "0.8.4"]
                                  [midje "1.9.9"]]
                   :plugins [[lein-cloverage "1.1.2"]
                             [lein-midje "3.2.1"]]}}
  :repl-options {:init-ns zero-one.geni.core})
