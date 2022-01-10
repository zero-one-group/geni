(defproject {{raw-name}} "0.0.1-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [zero.one/geni "0.0.39"]
                 [metosin/jsonista "0.3.3"
                  :exclusions [com.fasterxml.jackson.core/jackson-databind]]
                 [expound "0.8.9"]
                 ;; Spark
                 [org.apache.spark/spark-core_2.12 "3.1.2"]
                 [org.apache.spark/spark-hive_2.12 "3.1.2"]
                 [org.apache.spark/spark-mllib_2.12 "3.1.2"]
                 [org.apache.spark/spark-sql_2.12 "3.1.2"]
                 [org.apache.spark/spark-streaming_2.12 "3.1.2"]
                 [org.apache.spark/spark-yarn_2.12 "3.1.2"]
                 [com.github.fommil.netlib/all "1.1.2" :extension "pom"]
                 ; Arrow
                 [org.apache.arrow/arrow-memory-netty "4.0.0"]
                 [org.apache.arrow/arrow-memory-core "4.0.0"]
                 [org.apache.arrow/arrow-vector "4.0.0"
                  :exclusions [commons-codec com.fasterxml.jackson.core/jackson-databind]]
                 ;; Databases
                 [mysql/mysql-connector-java "8.0.25"]
                 [org.postgresql/postgresql "42.2.20"]
                 [org.xerial/sqlite-jdbc "3.34.0"]{{#dataproc?}}
                 ;; Dataproc
                 [org.apache.hadoop/hadoop-client "3.2.1"]
                 [com.google.guava/guava "27.0-jre"]{{/dataproc?}}{{#xgboost?}}
                 ;; Optional: Spark XGBoost
                 [ml.dmlc/xgboost4j-spark_2.12 "1.2.0"]
                 [ml.dmlc/xgboost4j_2.12 "1.2.0"]{{/xgboost?}}]{{#dataproc?}}:plugins [[lein-shell "0.5.0"]]
  :jar-name "{{raw-name}}.jar"
  :uberjar-name "{{raw-name}}-standalone.jar"
  :aliases {"spark-submit" ["do"
                            ["uberjar"]
                            ["shell"
                             "spark-submit"
                             "--class"
                             "{{namespace}}.core"
                             "target/uberjar/{{raw-name}}-standalone.jar"]]}{{/dataproc?}}
  :profiles {:uberjar {:aot :all}
             :dev {:plugins [[lein-ancient "0.7.0"]]}}
  :main ^:skip-aot {{namespace}}.core
  :target-path "target/%s")
