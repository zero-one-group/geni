(def spark-deps
  '[;; Spark
    ; This breaks cljcdoc: https://github.com/cljdoc/cljdoc/issues/407
    ; Frozen until issue is resolved.
    ;[com.github.fommil.netlib/all "1.1.2" :extension "pom"]
    [org.apache.spark/spark-avro_2.12 "3.0.0"]
    [org.apache.spark/spark-core_2.12 "3.0.0"]
    [org.apache.spark/spark-hive_2.12 "3.0.0"]
    [org.apache.spark/spark-mllib_2.12 "3.0.0"]
    [org.apache.spark/spark-sql_2.12 "3.0.0"]
    [org.apache.spark/spark-streaming_2.12 "3.0.0"]
    ; Databases
    [mysql/mysql-connector-java "8.0.21"]
    [org.postgresql/postgresql "42.2.14"]
    [org.xerial/sqlite-jdbc "3.32.3.1"]
    ;; Optional: Spark XGBoost
    [ml.dmlc/xgboost4j-spark_2.12 "1.0.0"]
    [ml.dmlc/xgboost4j_2.12 "1.0.0"]])

(defproject zero.one/geni "0.0.22"
  :jvm-opts ["-Duser.country=US" "-Duser.language=en"]
  :description "A Clojure library that wraps Apache Spark"
  :url "https://github.com/zero-one-group/geni"
  :license {:name "Apache License"
            :url  "https://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[camel-snake-kebab "0.4.1"]
                 [nrepl "0.8.0"]
                 [org.clojure/clojure "1.10.1"]
                 [org.clojure/java.data "1.0.86"]
                 [potemkin "0.4.5"]
                 [reply "0.4.4"]]
  :profiles
  {:provided {:dependencies ~spark-deps}
   :uberjar {:aot :all :dependencies ~spark-deps}
   :dev {:dependencies [[expound "0.8.5"]
                        [midje "1.9.9"]]
         :plugins [[lein-cloverage "1.1.2"]
                   [lein-midje "3.2.1"]]
         :aot [zero-one.geni.rdd.function
               zero-one.geni.aot-functions]}}
  :repl-options {:init-ns zero-one.geni.main}
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :main ^:skip-aot zero-one.geni.main
  :aliases {"coverage" ["cloverage"
                        "--fail-threshold"
                        "99"
                        "--codecov"
                        "--html"
                        "--runner"
                        ":midje"
                        "--ns-exclude-regex"
                        "zero-one.geni.main"]})
