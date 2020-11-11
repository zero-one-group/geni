(def spark-deps
  '[;; Spark
    ; This breaks cljcdoc: https://github.com/cljdoc/cljdoc/issues/407
    ; Frozen until issue is resolved.
    ;[com.github.fommil.netlib/all "1.1.2" :extension "pom"]
    [org.apache.spark/spark-avro_2.12 "3.0.1"]
    [org.apache.spark/spark-core_2.12 "3.0.1"]
    [org.apache.spark/spark-hive_2.12 "3.0.1"]
    [org.apache.spark/spark-mllib_2.12 "3.0.1"]
    [org.apache.spark/spark-sql_2.12 "3.0.1"]
    [org.apache.spark/spark-streaming_2.12 "3.0.1"]
    ; Arrow
    [org.apache.arrow/arrow-memory-netty "2.0.0"]
    [org.apache.arrow/arrow-memory-core "2.0.0"]
    [org.apache.arrow/arrow-vector "2.0.0"
     :exclusions [commons-codec com.fasterxml.jackson.core/jackson-databind]]
    ; Databases
    [mysql/mysql-connector-java "8.0.22"]
    [org.postgresql/postgresql "42.2.18"]
    [org.xerial/sqlite-jdbc "3.32.3.2"]
    ;; Optional: Spark XGBoost
    [ml.dmlc/xgboost4j-spark_2.12 "1.2.0"]
    [ml.dmlc/xgboost4j_2.12 "1.2.0"]])

(defproject zero.one/geni "0.0.35"
  :jvm-opts ["-Duser.country=US" "-Duser.language=en"]
  :description "A Clojure dataframe library that runs on Spark"
  :url "https://github.com/zero-one-group/geni"
  :license {:name "Apache License"
            :url  "https://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[camel-snake-kebab "0.4.2"]
                 [expound "0.8.6"]
                 [metosin/jsonista "0.2.7"
                  :exclusions [com.fasterxml.jackson.core/jackson-databind]]
                 [com.taoensso/nippy "3.1.0"]
                 [nrepl "0.8.3"]
                 [org.clojure/clojure "1.10.1"]
                 [org.clojure/java.data "1.0.86"]
                 [potemkin "0.4.5"]
                 [reply "0.4.4" :exclusions [javax.servlet/servlet-api]]
                 [zero.one/fxl "0.0.5"]]

  :profiles
  {:provided {:dependencies ~spark-deps}
   :uberjar {:aot :all :dependencies ~spark-deps}
   :dev {:dependencies [[criterium "0.4.6"]
                        [enlive "1.1.6"]
                        [midje "1.9.9"]
                        [techascent/tech.ml.dataset "5.00-alpha-22"
                         :exclusions [ch.qos.logback/logback-classic]]]
         :plugins [[lein-ancient "0.6.15"]
                   [lein-cloverage "1.2.1"]
                   [lein-midje "3.2.2"]
                   [lein-cljfmt "0.7.0"]]
         :cljfmt {:split-keypairs-over-multiple-lines?   false
                  :remove-multiple-non-indenting-spaces? false
                  :indents {facts [[:inner 0] [:block 1]]
                            fact  [[:inner 0] [:block 1]]}}
         :aot [zero-one.geni.rdd.function
               zero-one.geni.aot-functions]}}
  :repl-options {:init-ns zero-one.geni.main}
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :resource-paths ["resources"]
  :main ^:skip-aot zero-one.geni.main)
