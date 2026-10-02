(ns zero-one.geni.conf-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [zero-one.geni.core :as g]
   [zero-one.geni.test-resources :refer [spark]])
  (:import
   (org.apache.spark.sql AnalysisException)))

(deftest conf-test
  (try
    (testing "a set value, Spark's own default, or nil"
      (g/conf-set! "spark.sql.shuffle.partitions" 7)
      (is (= "7" (g/conf-get "spark.sql.shuffle.partitions")))
      (is (= "7" (g/conf-get @spark :spark.sql.shuffle.partitions)))
      (g/conf-unset! "spark.sql.shuffle.partitions")
      (is (= "200" (g/conf-get "spark.sql.shuffle.partitions")))
      (is (nil? (g/conf-get "geni.test.unknown"))))
    (testing "a default in place of Spark's"
      (is (= "5" (g/conf-get "spark.sql.shuffle.partitions" 5)))
      (is (= "fallback" (g/conf-get @spark "geni.test.unknown" "fallback")))
      (is (nil? (g/conf-get "geni.test.unknown" nil))))
    (testing "several values, of several types"
      (g/conf-set! @spark {:geni.test.flag true :geni.test.n 3 :geni.test.share 1.5 :geni.test.kw :x})
      (is (= ["true" "3" "1.5" "x"]
             (mapv g/conf-get ["geni.test.flag" "geni.test.n" "geni.test.share" "geni.test.kw"]))))
    (testing "what the session can set"
      (is (g/conf-modifiable? "spark.sql.ansi.enabled"))
      (is (not (g/conf-modifiable? @spark "spark.sql.warehouse.dir")))
      (is (not (g/conf-modifiable? "geni.test.flag")))
      (is (thrown? AnalysisException (g/conf-set! "spark.sql.warehouse.dir" "elsewhere"))))
    (finally
      (doseq [k ["spark.sql.shuffle.partitions" "geni.test.flag" "geni.test.n" "geni.test.share" "geni.test.kw"]]
        (g/conf-unset! k)))))
