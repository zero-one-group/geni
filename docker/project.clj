(defproject geni "0.0.1-SNAPSHOT"
  :description "A Clojure library that wraps Apache Spark"
  :url "https://gitlab.com/zero-one-open-source/geni"
  :license {:name "Apache License"
            :url  "https://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.apache.spark/spark-core_2.11 "2.4.5"]
                 [org.apache.spark/spark-hive_2.11 "2.4.5"]
                 [org.apache.spark/spark-sql_2.11 "2.4.5"]
                 [org.clojure/clojure "1.10.1"]]
  :profiles {:dev {:dependencies [[expound "0.8.4"]
                                  [midje "1.9.9"]]
                   :plugins [[lein-cloverage "1.1.2"]
                             [lein-midje "3.2.1"]]}}
  :repl-options {:init-ns geni.core})
