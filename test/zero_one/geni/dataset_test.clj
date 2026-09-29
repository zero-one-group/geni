(ns zero-one.geni.dataset-test
  (:require
   [clojure.set]
   [clojure.string]
   [clojure.test :refer [deftest is testing]]
   [zero-one.geni.core :as g]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.test-resources :refer [spark melbourne-df df-1 df-20 df-50 checkpoint-dir!]])
  (:import
   (org.apache.spark.rdd RDD)
   (org.apache.spark.sql Dataset
                         SparkSession
                         SQLContext)))

(deftest to-df-test
  (let [dataframe (g/select (df-1) :Suburb :Price)]
    (is (= (g/collect dataframe) (g/collect (g/to-df dataframe))))
    (is (= [:suburb :price] (g/columns (g/to-df dataframe [:suburb :price]))))))

(deftest ^:slow dataset-hints-test
  (is (clojure.string/includes? (-> (df-1)
                                    (g/hint "myHint" 100 true)
                                    .queryExecution
                                    .logical
                                    .toString) "myHint, [100, true]")))

(deftest clojure-idioms-test
  (let [r-50      (range 50)
        dataframe (g/records->dataset @spark (map (fn [i] {:x i}) r-50))]
    (is (= r-50 (-> dataframe (g/collect-col :x))))
    (let [actual (-> dataframe g/shuffle (g/collect-col :x))]
      (is (and (not= actual r-50)
               (= (set actual) (set r-50)))))))

(deftest join-with-test
  (let [base (df-50)
        n-listings (-> (df-50) (g/group-by :SellerG) g/count)]
    (is (= [:_1 :_2]
           (-> base
               (g/join-with
                n-listings
                (g/=== (g/col n-listings :SellerG)
                       (g/col-regex base :SellerG)))
               g/columns)))
    (is (= [:_1 :_2]
           (-> base
               (g/join-with
                n-listings
                (g/=== (g/col n-listings :SellerG)
                       (g/col base :SellerG))
                "left")
               g/columns)))))

(deftest non-group-by-aggregations-test
  (testing "On cube"
    (is (= 14 (-> (df-20) (g/cube :SellerG :Suburb) g/count g/count)))
    (is (= 13 (-> (df-20) (g/rollup :SellerG :Suburb) g/count g/count))))
  (testing "On grouping"
    (is (= 0
           (-> (df-20)
               (g/cube :SellerG :Suburb)
               (g/agg (g/grouping :SellerG))
               g/collect-vals
               first
               last)))))

(deftest alias-test
  (is (instance? Dataset (-> (df-50) (g/alias :abc)))))

(deftest na-methods-test
  (testing "On drop-na"
    (is (= 34 (-> (df-50) g/drop-na g/count)))
    (is (= 39 (-> (df-50) (g/drop-na 20) g/count)))
    (is (= 38 (-> (df-50) (g/drop-na [:BuildingArea]) g/count)))
    (is (= 38 (-> (df-50) (g/drop-na 1 [:BuildingArea]) g/count))))
  (testing "On fill-na"
    (is ((-> (df-50) (g/fill-na -999.0) (g/collect-col :BuildingArea) set) -999.0))
    (is (nil? ((-> (df-50) (g/fill-na -999.0 [:Regionname]) (g/collect-col :BuildingArea) set) -999.0))))
  (testing "On replace"
    (is ((-> (df-50) (g/replace-na :Rooms {1 -999}) (g/collect-col :Rooms) set) -999))))

(deftest ^:slow agg-methods-test
  (let [grouped (-> (df-50) (g/group-by :SellerG))]
    (is (= ["SellerG" "avg(Price)" "avg(Rooms)"] (-> grouped (g/mean :Price :Rooms) g/column-names)))
    (is (= ["SellerG" "min(Price)" "min(Rooms)"] (-> grouped (g/min :Price :Rooms) g/column-names)))
    (is (= ["SellerG" "max(Price)" "max(Rooms)"] (-> grouped (g/max :Price :Rooms) g/column-names)))
    (is (= ["SellerG" "sum(Price)" "sum(Rooms)"] (-> grouped (g/sum :Price :Rooms) g/column-names)))
    (is (= ["SellerG" "count"] (-> grouped g/count g/column-names)))))

(deftest ^:slow stats-functions-test
  (is (= [{:rooms 2 :seller "Biggin"} {:rooms 2 :seller "Jellis"}]
         (-> (df-20)
             (g/select {:seller :SellerG :rooms :Rooms})
             g/distinct
             (g/limit 5)
             (g/sample-by (g/struct :seller :rooms)
                          {["Biggin" 2] 1.0 ["Jellis" 2] 1.0}
                          36)
             g/collect)))
  (testing "On count-min-sketch"
    (let [count-min (g/count-min-sketch (melbourne-df) :Suburb 10 10 10)]
      (is (nil? (g/add count-min "abc")))
      (is (nil? (g/add count-min "abc" 10)))
      (is (< 0.9 (g/confidence count-min)))
      (is (= 10 (g/depth count-min)))
      (is (< 700 (g/estimate-count count-min "Abbotsford") 775))
      (is (< (g/relative-error count-min) 0.3))
      (is (interop/array? (g/to-byte-array count-min)))
      (is (< 10000 (g/total-count count-min)))
      (is (= 10 (g/width count-min)))))
  (testing "On cov"
    (is (< 290000 (g/cov (melbourne-df) :Price :Rooms) 310000)))
  (testing "On corr"
    (is (< 0.45 (g/corr (melbourne-df) :Price :Rooms) 0.55))
    (is (< 0.45 (g/corr (melbourne-df) :Price :Rooms "pearson") 0.55)))
  (testing "On cross-tab"
    (is (= [{:Biggin 9
             :Collins 1
             :Greg 1
             :Jellis 4
             :LITTLE 1
             :Nelson 4
             :Suburb_SellerG "Abbotsford"}]
           (-> (df-20)
               (g/crosstab :Suburb :SellerG)
               g/collect))))
  (testing "On freq-items"
    ;; Spark doesn't guarantee the order of the frequent items.
    (is (= {:SellerG_freqItems #{"LITTLE"
                                 "Biggin"
                                 "Nelson"
                                 "Collins"
                                 "Greg"
                                 "Jellis"}
            :Suburb_freqItems #{"Abbotsford"}}
           (-> (df-20)
               (g/freq-items [:Suburb :SellerG])
               g/collect
               first
               (update-vals set))))
    (is (= {:SellerG_freqItems #{"Biggin" "Collins"}
            :Suburb_freqItems #{"Abbotsford"}}
           (-> (df-20)
               (g/freq-items [:Suburb :SellerG] 0.5)
               g/collect
               first
               (update-vals set)))))
  (testing "On bloom-filter"
    (let [bloom (-> (melbourne-df) (g/bloom-filter :Suburb 10 0.01))]
      (is (= 128 (g/bit-size bloom)))
      (is (g/compatible? bloom bloom))
      (is (= 1.0 (g/expected-fpp bloom)))
      (is (instance? (class bloom) (g/merge-in-place bloom bloom)))
      (is (boolean? (g/might-contain bloom "Reservoir")))
      (is (boolean? (g/put bloom "xyz")))))
  (testing "On approx-quantile"
    (let [actual (-> (melbourne-df)
                     (g/approx-quantile :Price [0.1 0.9] 0.2))]
      (is (< (first actual) (second actual))))
    (let [actual (-> (melbourne-df)
                     (g/approx-quantile [:Price] [0.1 0.9] 0.2))]
      (is (< (ffirst actual) (second (first actual)))))))

(deftest ^:slow random-split-test
  (let [[train-df val-df] (-> (df-50) (g/random-split [90 10]))]
    (is (true? (< (g/count val-df)
                  (g/count train-df)))))
  (let [[train-df val-df] (-> (df-50) (g/random-split [90 10] 123))]
    (is (true? (< (g/count val-df)
                  (g/count train-df))))))

(deftest printing-functions-test
  (testing "should return nil"
    (let [n-lines   #(-> % clojure.string/split-lines count)
          df        (g/select (melbourne-df) :Suburb :Address)
          n-columns (-> df g/column-names count)]
      (is (= 7 (n-lines (with-out-str (g/show (g/limit df 3))))))
      (is (= 10 (n-lines (with-out-str (g/show df {:num-rows 3 :vertical true})))))
      (is (= 9 (n-lines (with-out-str (g/show-vertical (g/limit df 3))))))
      (is (= 10 (n-lines (with-out-str (g/show-vertical df {:num-rows 3})))))
      (is (= (inc n-columns) (n-lines (with-out-str (g/print-schema df)))))
      (is (< 1 (n-lines (interop/with-scala-out-str (g/explain df)))))
      (is (< 10 (n-lines (interop/with-scala-out-str (g/explain df true))))))))

(deftest dtypes-test
  (is (= "StringType" (-> (melbourne-df) g/dtypes :Suburb))))

(deftest local-test
  (is (boolean? (-> (melbourne-df) g/local?))))

(deftest ungrouped-methods-test
  (is (false? (-> (melbourne-df) g/streaming?)))
  (is (instance? SparkSession (-> (melbourne-df) g/spark-session)))
  (is (instance? SQLContext (-> (melbourne-df) g/sql-context)))
  (is ((every-pred seq? #(every? string? %)) (-> (df-1) g/to-json g/collect)))
  (is (string? (-> (df-1) g/to-string)))
  (is (= ["{\"Date\":\"3/12/2016\",\"CouncilArea\":\"Yarra\"}"
          "{\"Date\":\"4/02/2016\",\"CouncilArea\":\"Yarra\"}"]
         (-> (df-20)
             (g/limit 2)
             (g/select :Date :CouncilArea)
             g/to-json
             g/collect)))
  (is (= (-> (df-1) g/to-json g/collect) (-> (df-1) g/to-json g/collect))))

(deftest ^:slow pivot-test
  (testing "pivot should return the expected cols"
    (let [pivotted (-> (df-20)
                       (g/group-by :SellerG)
                       (g/pivot :Method)
                       (g/agg (-> (g/count "*") (g/as "n"))))]
      (is (= #{"SellerG" "PI" "S" "SP" "VB"} (-> pivotted g/column-names set)))))
  (testing "pivot should be able to specify pivot columns"
    (let [pivotted (-> (df-20)
                       (g/group-by :SellerG)
                       (g/pivot :Method ["SP" "VB" "XYZ"])
                       (g/agg (-> (g/count "*") (g/as "n"))))]
      (is (= #{"SellerG" "SP" "VB" "XYZ"} (-> pivotted g/column-names set))))))

(deftest when-test
  (testing "when null and coalesce should be equivalent"
    (is (every? identity (-> (df-20)
                             (g/with-column "x"
                               (g/when (g/null? :BuildingArea) -999 :BuildingArea))
                             (g/with-column "y"
                               (g/coalesce :BuildingArea -999))
                             (g/select (g/=== "x" "y"))
                             g/collect-vals
                             flatten)))))

(deftest select-test
  (testing "should drop unselected columns"
    (is (= ["Type" "Price" "Regionname" "a" "b"]
           (-> (melbourne-df)
               (g/select :Type (g/col :Price) :Regionname {:a :SellerG :b :BuildingArea})
               g/column-names))))
  (testing "select-expr works as expected"
    (is (= ["(Price + 1)" "(Rooms - 1)"]
           (-> (melbourne-df)
               (g/select-expr "Price+1" "Rooms-1")
               g/column-names))))
  (testing "column order should be preserved"
    (is (= (range 100)
           (-> (melbourne-df)
               (g/select (range 100))
               g/collect-vals
               first)))))

(deftest filter-test
  (let [df (-> (df-20) (g/select :SellerG))]
    (testing "should implicitly cast to boolean"
      (is (not ((-> (df-20)
                    (g/select :Rooms)
                    (g/filter (g/- :Rooms 2))
                    g/distinct
                    (g/collect-col :Rooms)
                    set) 2)))
      (is (= [2]
             (-> (df-20)
                 (g/select :Rooms)
                 (g/remove (g/- :Rooms 2))
                 g/distinct
                 (g/collect-col :Rooms)))))
    (testing "should correctly filter rows"
      (is (= [{:SellerG "Biggin"}]
             (-> df
                 (g/filter (g/=== :SellerG (g/lit "Biggin")))
                 g/distinct
                 g/collect))))
    (testing "should filter correctly with isin"
      (is (= #{"Greg" "Collins" "Biggin"}
             (-> df
                 (g/filter (g/isin :SellerG ["Greg" "Collins" "Biggin"]))
                 g/distinct
                 g/collect-vals
                 flatten
                 set)))
      (is (empty? (clojure.set/intersection (-> df
                                                (g/filter (g/not (g/isin :SellerG ["Greg" "Collins" "Biggin"])))
                                                g/distinct
                                                g/collect-vals
                                                flatten
                                                set) #{"Greg" "Collins" "Biggin"}))))
    (testing "should correctly remove rows"
      (is (= #{"Nelson" "Jellis" "Greg" "LITTLE" "Collins"}
             (-> df
                 (g/remove (g/=== :SellerG (g/lit "Biggin")))
                 (g/collect-col :SellerG)
                 distinct
                 set))))))

(deftest rename-columns-test
  (testing "the new name should exist and the old name should not"
    (let [col-names (-> (melbourne-df)
                        (g/rename-columns {:Regionname :region-name})
                        g/columns
                        set)]
      (is (contains? col-names :region-name))
      (is (not (contains? col-names :Regionname)))))
  (testing "with-column-renamed actually renames column"
    (is (nil? ((-> (df-1)
                   (g/with-column-renamed :SellerG :seller)
                   g/columns
                   set) :SellerG)))))

(deftest ^:slow actions-test
  (testing "correct collection of lits"
    (is (= [1 "a" [2.0] ["b"]]
           (-> (df-1)
               (g/select
                (g/lit 1)
                (g/lit "a")
                (g/lit [2.0])
                (g/lit ["b"]))
               g/first-vals))))
  (testing "action functions work"
    (is (map? (g/head (df-20))))
    (is (= (count (g/head (df-20) 2)) 2))
    (is (vector? (g/head-vals (df-20))))
    (let [actual (g/head-vals (df-20) 3)]
      (is (and (= (count actual) 3) (every? vector? actual))))
    (let [actual (g/take (df-20) 5)]
      (is (and (= (count actual) 5) (every? map? actual))))
    (let [actual (g/take-vals (df-20) 10)]
      (is (and (= (count actual) 10) (every? vector? actual))))
    (let [actual (g/tail (df-20) 5)]
      (is (and (= (count actual) 5) (every? map? actual))))
    (let [actual (g/tail-vals (df-20) 10)]
      (is (and (= (count actual) 10) (every? vector? actual)))))
  (testing "first works"
    (is (= {:Address "85 Turner St"} (-> (df-20) (g/select :Address) g/first)))
    (is (= ["85 Turner St"] (-> (df-20) (g/select :Address) g/first-vals)))
    (is (= {:Address "42 Valiant St"} (-> (df-20) (g/select :Address) g/last)))
    (is (= ["42 Valiant St"] (-> (df-20) (g/select :Address) g/last-vals)))))

(deftest ^:slow drop-test
  (testing "dropped columns should no longer exist"
    (let [original-columns (-> (melbourne-df) g/columns set)
          columns-to-drop  #{:Suburb :Price :YearBuilt}
          dropped-columns  (-> (melbourne-df)
                               (g/drop columns-to-drop)
                               g/columns
                               set)]
      (is (clojure.set/subset? columns-to-drop original-columns))
      (is (empty? (clojure.set/intersection columns-to-drop dropped-columns)))))
  (testing "drop duplicates without arg should not drop everything"
    (is (= 10
           (-> (df-20)
               (g/select :Method :SellerG)
               g/drop-duplicates
               g/count))))
  (testing "drop duplicates can be called with columns"
    (is (= 6
           (-> (df-20)
               (g/select :Method :SellerG)
               (g/drop-duplicates :SellerG)
               g/count)))))

(deftest ^:slow except-and-intercept-test
  (testing "except should exclude the row"
    (is (= 19
           (-> (df-20)
               (g/union (df-20))
               (g/except (df-1))
               g/count))))
  (testing "except all should leave out the duplicates"
    (is (= 39
           (-> (df-20)
               (g/union (df-20))
               (g/except-all (df-1))
               g/count))))
  (testing "except then intercept should be empty"
    (is (true? (-> (df-20)
                   (g/except (df-1))
                   (g/intersect (df-1))
                   g/empty?))))
  (testing "intersect all should preserve duplicates"
    (is (= 1
           (-> (df-20)
               (g/union (df-20))
               (g/intersect-all (df-1))
               g/count))))) ; TODO: this should be 2

(deftest ^:slow union-test
  (testing "Union should double the rows preserve distinctness"
    (let [unioned (g/union (df-20) (df-20) (df-20))]
      (is (= 60 (g/count unioned)))
      (is (= 20 (-> unioned g/distinct g/count)))))
  (testing "Union by name should line up the names"
    (is (= 1
           (let [left (-> (df-1) (g/select :Suburb :SellerG))
                 right (-> (df-1) (g/select :SellerG :Suburb))]
             (-> left (g/union-by-name right right) g/distinct g/count))))))

(deftest ^:slow describe-test
  (testing "describe should have the right shape"
    (let [summary (-> (df-20) (g/describe :Price))]
      (is (= ["summary" "Price"] (g/column-names summary)))
      (is (= ["count" "mean" "stddev" "min" "max"] (map :summary (g/collect summary))))))
  (testing "summary should only pick some stats"
    (is (= [["count" "20"] ["min" "1"]]
           (-> (df-20)
               (g/select :Rooms)
               (g/summary "count" "min")
               g/collect-vals)))))

(deftest ^:slow sample-test
  (let [with-rep    (g/sample (df-50) 0.8 true)
        without-rep (g/sample (df-50) 0.8)]
    (testing "Sampling without replacement should have all unique rows"
      (is (= (g/count without-rep) (-> without-rep g/distinct g/count))))
    (testing "Sampling with replacement should have less unique rows"
      (is (< (-> with-rep g/distinct g/count) 40)))))

(deftest ^:slow order-by-test
  (let [df (-> (df-20) (g/select (g/as (g/->date-col :Date "d/MM/yyyy") :Date)))]
    (testing "should correctly order dates - desc"
      (let [records (-> df (g/order-by (g/desc :Date)) g/collect)
            dates   (map #(str (% :Date)) records)]
        (is (every? (complement neg?) (map compare dates (rest dates))))))
    (testing "should correctly order dates - asc"
      (let [records (-> df (g/order-by (g/asc :Date)) g/collect)
            dates   (map #(str (% :Date)) records)]
        (is (every? (complement pos?) (map compare dates (rest dates))))))))

(deftest ^:slow caching-test

  (let [df (-> (df-1) g/cache)]
    (is (true? (.useMemory (g/storage-level df)))))
  (let [df (-> (df-1) g/persist)]
    (is (true? (.useMemory (g/storage-level df)))))
  (let [df (-> (df-1) g/persist)]
    (is (true? (.useMemory (g/storage-level df)))))
  (let [df (-> (df-1) g/persist g/unpersist)]
    (is (false? (.useMemory (g/storage-level df)))))
  (let [df (-> (df-1) g/persist (g/unpersist true))]
    (is (false? (.useMemory (g/storage-level df)))))
  (is (= g/memory-only-ser-2
         (let [df (g/persist (df-1) g/memory-only-ser-2)]
           (g/storage-level df))))
  (is (seq? (g/input-files (melbourne-df))))
  (is (instance? RDD (g/rdd (melbourne-df))))
  (let [checkpointed? (fn [df] (-> df
                                   .queryExecution
                                   .toRdd
                                   .toDebugString
                                   (clojure.string/includes? "CheckpointRDD")))]
    (checkpoint-dir!)
    (is (not (checkpointed? (df-1))))
    (is (checkpointed? (g/checkpoint (df-1))))
    (is (checkpointed? (g/checkpoint (df-1) true)))))

(deftest ^:slow repartition-test
  (testing "able to repartition by a number"
    (is (= 2
           (-> (df-20)
               (g/repartition 2)
               g/partitions
               count))))
  (testing "able to repartition by columns"
    (is (<= 1 (-> (df-20)
                  (g/repartition :Suburb :SellerG)
                  g/partitions
                  count))))
  (testing "able to repartition by number and columns"
    (is (= 10
           (-> (df-20)
               (g/repartition 10 :Suburb :SellerG)
               g/partitions
               count))))
  (testing "able to repartition by range by columns"
    (is (pos?
         (-> (df-20)
             (g/repartition-by-range :Suburb :SellerG)
             g/partitions
             count))))
  (testing "able to repartition by range by number and columns"
    (is (= 3
           (-> (df-20)
               (g/repartition-by-range 3 :Suburb :SellerG)
               g/partitions
               count))))
  (testing "sort within partitions is differnt to sort"
    (let [sorted  (-> (df-20)
                      (g/select :Method :SellerG)
                      (g/order-by :Method)
                      g/collect-vals)
          sorted-within (-> (df-20)
                            (g/select :Method :SellerG)
                            (g/repartition 2 :SellerG)
                            (g/sort-within-partitions :Method)
                            g/collect-vals)]
      (is (false? (= sorted sorted-within)))
      (is (= (set sorted-within) (set sorted)))))
  (testing "coalesce should reduce the number of partitions"
    (is (= 2
           (-> (df-20)
               (g/repartition 5)
               (g/coalesce 2)
               g/partitions
               count)))))

(deftest ^:slow join-test
  (testing "joining with join exprs"
    (let [left (df-1)
          right (df-50)]
      (is (= 38
             (-> left
                 (g/join right
                         (g/= (g/col right :Suburb)
                              (g/col left :Suburb))
                         "inner")
                 g/count)))))
  (testing "normal join works as expected"
    (let [grouped (-> (df-50)
                      (g/group-by :SellerG :Regionname)
                      (g/agg {:mean-price (g/mean :Price)}))]
      (is (contains? (-> (df-50) (g/join grouped [:SellerG :Regionname]) g/columns set) :mean-price))))
  (testing "normal join works as expected"
    (let [n-listings (-> (df-50)
                         (g/group-by :Suburb)
                         (g/agg (g/as (g/count "*") :n-listings)))]
      (is (contains? (-> (df-50) (g/join n-listings :Suburb) g/columns set) :n-listings))
      (is (contains? (-> (df-50) (g/join n-listings :Suburb "inner") g/columns set) :n-listings))
      (is (contains? (-> (df-50) (g/join n-listings [:Suburb] "inner") g/columns set) :n-listings))))
  (testing "cross-join works as expected"
    (is (= 400
           (-> (df-20)
               (g/select :Suburb)
               (g/cross-join (-> (df-20) (g/select :Method)))
               g/count)))))

(deftest ^:slow group-by-and-agg-test
  (testing "group-by with map"
    (is (= [:seller :rooms :mean-price]
           (-> (df-20)
               (g/group-by {:seller :SellerG :rooms :Rooms})
               (g/agg {:mean-price (g/mean :Price)})
               g/columns))))
  (testing "group-by with map"
    (is (= ["SellerG" "n-regions" "n-null-building-area"]
           (-> (df-20)
               (g/group-by :SellerG)
               (g/agg {:n-regions (g/count-distinct :Regionname)
                       :n-null-building-area (g/null-count :BuildingArea)})
               g/column-names))))
  (testing "should have the right shape"
    (let [agged (-> (df-50)
                    (g/group-by :Type)
                    (g/agg
                     (-> (g/count "*") (g/as "n_rows"))
                     (-> (g/max :Price) (g/as "max_price"))))]
      (is (= (-> (df-50) (g/select :Type) g/distinct g/count) (g/count agged)))
      (is (= ["Type" "n_rows" "max_price"] (g/column-names agged)))))
  (testing "agg-all should apply to all columns"
    (is (= 3
           (-> (df-20)
               (g/select :Price :Regionname :Car)
               (g/agg-all g/count-distinct)
               g/collect
               first
               count))))
  (testing "works with nested data structure"
    (let [agged    (-> (df-20)
                       (g/group-by :SellerG)
                       (g/agg
                        (-> (g/collect-list :Suburb) (g/as "suburbs_list"))
                        (-> (g/collect-set :Suburb) (g/as "suburbs_set"))))
          exploded (g/with-column agged "exploded" (g/explode "suburbs_list"))]
      (is (< (g/count agged) 20))
      (is (= 20 (g/count exploded))))))

(deftest sparse-vector-test
  (testing "collects sparse data"
    (let [sparse-df
          (g/create-dataframe
           [(g/row (g/sparse 4 [1 3] [3.0 4.0]))]
           {:test :vector})]
      (is (= [{:size 4 :indices [1 3] :values [3.0 4.0]}] (g/collect-col sparse-df :test))))))
