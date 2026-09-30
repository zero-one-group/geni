(ns ^:connect zero-one.geni.connect-test
  "Geni over Spark Connect, with Spark's JVM client in place of classic Spark:
  clojure -T:build connect-tests. The rest of the suite runs here too, except
  the tests marked ^:classic."
  (:require
   [clojure.test :refer [deftest is testing]]
   [zero-one.geni.core :as g]
   [zero-one.geni.test-resources :as tr :refer [spark]])
  (:import
   (clojure.lang ExceptionInfo)
   (org.apache.spark.sql SparkSession)))

(defn- connect-session? [session]
  (= "org.apache.spark.sql.connect.SparkSession" (.getName (class session))))

(deftest default-session-test
  (testing "Geni's default session connects to SPARK_REMOTE"
    (is (connect-session? @spark))
    (is (identical? @spark (.sparkSession (g/range 3)))))
  (testing "and has the server's configs and version"
    (is (string? (:spark.app.name (g/spark-conf @spark))))
    (is (re-find #"^4\." (g/version)))))

(deftest connect-test
  (let [before  @spark
        session (g/connect (System/getenv "SPARK_REMOTE")
                           {:configs {:spark.sql.shuffle.partitions 7}})]
    (try
      (testing "g/connect starts a session that Geni then uses"
        (is (connect-session? session))
        (is (not (identical? before session)))
        (is (identical? session @spark)))
      (testing "with the given configs"
        (is (= "7" (:spark.sql.shuffle.partitions (g/spark-conf session)))))
      (testing "and queries run on it"
        (is (= [{:k 0 :n 2} {:k 1 :n 1}]
               (-> (g/records->dataset [{:x 1} {:x 2} {:x 4}])
                   (g/group-by (g/mod :x 2))
                   (g/agg {:n (g/count "*")})
                   (g/select {:k "(x % 2)" :n :n})
                   (g/order-by :k)
                   g/collect))))
      (finally
        ;; Closed sessions shut their channels, which gRPC logs about otherwise.
        (.close ^SparkSession session)
        (.close ^SparkSession before)
        (tr/reset-session!)))))

(deftest connect-after-set-default-session-test
  (tr/stop-session!)
  (let [chosen  (g/set-default-session! (g/connect))
        session (g/connect)]
    (try
      (is (identical? session @spark))
      (finally
        (.close ^SparkSession chosen)
        (.close ^SparkSession session)
        (tr/reset-session!)))))

(deftest classic-only-test
  (testing "Functions that need a SparkContext say so"
    (is (thrown-with-msg? ExceptionInfo #"needs a classic SparkSession"
                          (g/java-spark-context @spark)))
    (is (thrown-with-msg? ExceptionInfo #"needs a classic SparkSession"
                          (g/app-name))))
  (testing "Spark itself says so for RDDs"
    (is (thrown-with-msg? UnsupportedOperationException #"UNSUPPORTED_CONNECT_FEATURE"
                          (g/rdd (g/range 3)))))
  (testing "create-spark-session points to g/connect"
    (is (thrown-with-msg? ExceptionInfo #"g/connect"
                          (g/create-spark-session {}))))
  (testing "UDFs say that they need classic Spark"
    (is (thrown-with-msg? ExceptionInfo #"UDFs need classic Spark" (g/udf inc :long)))
    (is (thrown-with-msg? ExceptionInfo #"UDFs need classic Spark"
                          (g/register-udf! "plus_one" inc :long))))
  (testing "MLlib's vectors and Geni's Arrow export name what they need"
    (is (thrown-with-msg? ExceptionInfo #"spark-mllib" (g/dense 1.0 2.0)))
    (is (thrown-with-msg? ExceptionInfo #"spark-mllib" (g/corr (g/range 3) :id)))
    (is (thrown-with-msg? ExceptionInfo #"arrow-vector"
                          (g/collect-to-arrow (g/range 3) 10 "target/arrow")))))
