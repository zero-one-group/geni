(defproject zero.one/geni "0.0.1-SNAPSHOT"
  :description "A Clojure library that wraps Apache Spark"
  :url "https://github.com/zero-one-group/geni"
  :license {:name "Apache License"
            :url  "https://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [camel-snake-kebab "0.4.1"]
                 [potemkin "0.4.5"]]
  :profiles {:provided
             {:dependencies [[org.apache.spark/spark-core_2.12 "2.4.5"]
                             [org.apache.spark/spark-hive_2.12 "2.4.5"]
                             [org.apache.spark/spark-mllib_2.12 "2.4.5"]
                             [org.apache.spark/spark-sql_2.12 "2.4.5"]
                             [org.apache.spark/spark-streaming_2.12 "2.4.5"]]}
             :dev {:dependencies [[expound "0.8.4"]
                                  [midje "1.9.9"]]
                   :plugins [[lein-cloverage "1.1.2"]
                             [lein-midje "3.2.1"]]}}
  :repl-options {:init-ns zero-one.geni.core})
