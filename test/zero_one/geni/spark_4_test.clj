(ns zero-one.geni.spark-4-test
  "The verbs that need Spark 4, which give an error naming the version they
  need on an older Spark."
  (:require
   [clojure.test :refer [deftest is testing]]
   [zero-one.geni.catalog :as c]
   [zero-one.geni.core :as g]
   [zero-one.geni.spark :as spark]
   [zero-one.geni.test-resources :refer [spark spark-at-least? with-fresh-session]])
  (:import
   (clojure.lang ExceptionInfo)
   (java.util.regex Pattern)
   (org.apache.spark.sql AnalysisException)))

(defmacro ^:private with-spark-at-least
  "Runs `body` when the Spark on the classpath is at least `needed`, and
  otherwise checks that `form` throws an error naming that version."
  [needed form & body]
  `(if (spark-at-least? ~needed)
     (do ~@body)
     (is (~'thrown-with-msg? ExceptionInfo
                             ~(re-pattern (str "needs Spark " (Pattern/quote needed)))
                             ~form))))

(defn- sales []
  (g/records->dataset @spark [{:k "a" :x 1 :y 3} {:k "b" :x 2 :y 4}]))

(deftest transpose-test
  (with-spark-at-least "4.0" (g/transpose (sales))
    (is (= [{:key "x" :a 1 :b 2} {:key "y" :a 3 :b 4}] (-> (sales) g/transpose (g/order-by :key) g/collect)))
    (is (= [{:key "x" :a 1 :b 2} {:key "y" :a 3 :b 4}]
           (-> (sales) (g/transpose :k) (g/order-by :key) g/collect)))))

(deftest grouping-sets-test
  (with-spark-at-least "4.0" (g/grouping-sets (sales) [[:k] []] :k)
    (is (= [{:k nil :total 3} {:k "a" :total 1} {:k "b" :total 2}]
           (-> (sales)
               (g/grouping-sets [[:k] []] :k)
               (g/agg {:total (g/sum :x)})
               (g/order-by :k)
               g/collect)))))

(deftest lateral-join-test
  (with-spark-at-least "4.0" (g/outer :x)
    (let [rows (fn [n] (g/select (g/range n) {:z (g/+ :id (g/outer :x))}))]
      (is (= [[1 1] [1 2] [2 2] [2 3]]
             (-> (sales) (g/lateral-join (rows 2)) (g/order-by :x :z) (g/select :x :z) g/collect-vals)))
      (is (= [[1 3] [2 3] [2 4]]
             (-> (sales) (g/lateral-join (rows 3) (g/> :z 2)) (g/order-by :x :z) (g/select :x :z) g/collect-vals)))
      (is (= [[1 nil] [2 3]]
             (-> (sales)
                 (g/lateral-join (g/filter (rows 2) (g/> :z 2)) (g/lit true) :left)
                 (g/order-by :x)
                 (g/select :x :z)
                 g/collect-vals)))
      (is (= [[1 1] [2 2]]
             (-> (sales) (g/lateral-join (rows 1) :cross) (g/order-by :x) (g/select :x :z) g/collect-vals)))
      (testing "a keyword that names no join type is a column"
        (is (= [[1 2] [2 2] [2 3]]
               (-> (sales)
                   (g/lateral-join (g/with-column (rows 2) :big (g/> :z 1)) :big)
                   (g/order-by :x :z)
                   (g/select :x :z)
                   g/collect-vals)))))))

(deftest subquery-test
  (with-spark-at-least "4.0" (g/scalar (sales))
    (is (= ["b"] (-> (sales) (g/filter (g/> :x (g/scalar (g/agg (sales) (g/min :x))))) (g/collect-col :k))))
    (is (= ["a"] (-> (sales)
                     (g/filter (g/exists (g/filter (g/range 2) (g/=== :id (g/outer :x)))))
                     (g/collect-col :k)))))
  (testing "exists on an array column, on any Spark"
    (is (= [true] (-> (g/records->dataset @spark [{:xs [1 5]}])
                      (g/select {:big (g/exists :xs #(g/> % 3))})
                      (g/collect-col :big)))))
  (with-spark-at-least "4.1" (g/isin :x (g/range 2))
    (is (= ["a"] (-> (sales) (g/filter (g/isin :x (g/select (g/range 2) :id))) (g/collect-col :k))))))

(deftest try-cast-test
  (with-spark-at-least "4.0" (g/try-cast :s "int")
    (is (= [1 nil] (-> (g/records->dataset @spark [{:s "1"} {:s "x"}])
                       (g/select {:n (g/try-cast :s "int")})
                       (g/collect-col :n))))))

(deftest zip-with-index-test
  (with-spark-at-least "4.2" (g/zip-with-index (sales))
    (is (= [["a" 0] ["b" 1]]
           (-> (sales) (g/order-by :k) g/zip-with-index (g/select :k :index) g/collect-vals)))
    (is (= [:k :x :y :row] (g/columns (g/zip-with-index (sales) :row))))))

(deftest nearest-by-join-test
  (let [queries  (g/records->dataset @spark [{:qid 1 :q 1.0} {:qid 2 :q 5.0}])
        items    (g/records->dataset @spark [{:item 10 :v 0.9} {:item 11 :v 4.0} {:item 12 :v 6.0}])
        distance (g/abs (g/- :q :v))
        options  {:num-results 1 :mode :exact :direction :distance}]
    (with-spark-at-least "4.2" (g/nearest-by-join queries items distance options)
      (is (= [[1 10] [2 11]]
             (-> (g/nearest-by-join queries items distance options)
                 (g/order-by :qid)
                 (g/select :qid :item)
                 g/collect-vals)))
      (is (= [[1 nil] [2 nil]]
             (-> (g/nearest-by-join queries (g/filter items (g/> :v 100)) distance
                                    (assoc options :join-type :left))
                 (g/order-by :qid)
                 (g/select :qid :item)
                 g/collect-vals)))
      (is (thrown-with-msg? ExceptionInfo #":num-results, :mode and :direction"
                            (g/nearest-by-join queries items distance {:num-results 1}))))))

(deftest table-function-test
  (is (= [1 2 3] (g/collect-col (g/table-function :explode [[1 2 3]]) :col)))
  (is (= [1 2] (g/collect-col (g/table-function @spark :range [1 3]) :id)))
  (is (= [[1 "a"] [2 "b"]] (g/collect-vals (g/table-function :stack [(int 2) 1 "a" 2 "b"]))))
  (is (pos? (g/count (g/table-function :sql-keywords))))
  (when (spark-at-least? "4.0")
    (is (= [{:a 1 :b "x"}]
           (g/collect (g/table-function :inline [(g/array (g/struct (g/as (g/lit 1) :a)
                                                                    (g/as (g/lit "x") :b)))])))))
  (is (thrown-with-msg? ExceptionInfo #"table-valued function's name"
                        (g/table-function "range(1); DROP TABLE t" []))))

(deftest sql-with-many-positional-args-test
  (if (#{"4.1.0" "4.1.1" "4.1.2" "4.1.3" "4.2.0"} (.version @spark))
    (is (thrown-with-msg? ExceptionInfo #"SPARK-58341"
                          (g/sql @spark "SELECT ? AS a, ? AS b, ? AS c, ? AS d, ? AS e" [1 2 3 4 5])))
    (is (= [{:a 1 :b 2 :c 3 :d 4 :e 5}]
           (g/collect (g/sql @spark "SELECT ? AS a, ? AS b, ? AS c, ? AS d, ? AS e" [1 2 3 4 5])))))
  (is (= [{:a 1 :b 2 :c 3 :d 4}]
         (g/collect (g/sql @spark "SELECT ? AS a, ? AS b, ? AS c, ? AS d" [1 2 3 4]))))
  (testing "the versions it covers, with a vendor's version without a patch number as its first"
    (let [misbound? #(#'spark/positional-args-misbound? % 5)]
      (is (every? misbound? ["4.1.0" "4.1.3" "4.1" "4.2.0" "4.2" "4.2.0-vendor"]))
      (is (not-any? misbound? ["3.5.9" "4.0.1" "4.1.4" "4.2.1"]))
      (is (not (#'spark/positional-args-misbound? "4.2" 4))))))

(deftest clustered-tables-test
  (let [clustering (fn [table-name]
                     (->> (g/collect (g/sql @spark (str "SHOW TBLPROPERTIES " table-name)))
                          (filter #(= "clusteringColumns" (:key %)))
                          (map :value)))]
    (with-spark-at-least "4.0" (g/write-table! (sales) "clustered" {:cluster-by :k}))
    (with-spark-at-least "4.0" (g/write-to! (sales) "v2" {:mode :create :cluster-by :k})
      (with-fresh-session
        (g/write-table! (sales) "clustered" {:format :parquet :cluster-by :k})
        (is (= ["[[\"k\"]]"] (clustering "clustered")))
        (g/write-to! (sales) "v2" {:mode :create :using "parquet" :cluster-by [:k :x]})
        (is (= ["[[\"k\"],[\"x\"]]"] (clustering "v2")))
        (is (c/table-exists? "v2"))))))

(deftest read-changes-test
  (with-spark-at-least "4.2" (g/read-changes! "anything")
    (with-fresh-session
      (g/write-table! (sales) "plain")
      (is (thrown-with-msg? AnalysisException #"Change Data Capture"
                            (g/collect (g/read-changes! "plain" {:starting-version 0})))))))
