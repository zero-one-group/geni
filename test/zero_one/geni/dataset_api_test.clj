(ns zero-one.geni.dataset-api-test
  (:require
    [clojure.set]
    [clojure.string]
    [midje.sweet :refer [facts fact =>]]
    [zero-one.geni.core :as g]
    [zero-one.geni.interop :as interop]
    [zero-one.geni.test-resources :refer [melbourne-df df-1 df-20 df-50]])
  (:import
    (org.apache.spark.sql Dataset
                          SparkSession
                          SQLContext)))

(fact "On join-with"
  (let [n-listings (-> df-50 (g/group-by "SellerG") g/count)]
    (-> df-50
        (g/join-with
          n-listings
          (g/=== (g/col n-listings "SellerG")
                 (g/col-regex df-50 "SellerG")))
        g/column-names) => ["_1" "_2"]
    (-> df-50
        (g/join-with
          n-listings
          (g/=== (g/col n-listings "SellerG")
                 (g/col df-50 "SellerG"))
          "left")
        g/column-names) => ["_1" "_2"]))

(facts "On non-group-by aggregations"
  (fact "On cube"
    (-> df-20 (g/cube "SellerG" "Suburb") g/count g/count) => 14
    (-> df-20 (g/rollup "SellerG" "Suburb") g/count g/count) => 13)
  (fact "On grouping"
    (-> df-20
        (g/cube "SellerG" "Suburb")
        (g/agg (g/grouping "SellerG"))
        g/collect-vals
        first
        last) => 0))

(fact "On alias"
  (-> df-50 (g/alias "abc")) => (partial instance? Dataset))

(facts "On NA methods"
  (fact "On drop-na"
    (-> df-50 g/drop-na g/count) => 34
    (-> df-50 (g/drop-na 20) g/count) => 39
    (-> df-50 (g/drop-na ["BuildingArea"]) g/count) => 38
    (-> df-50 (g/drop-na 1 ["BuildingArea"]) g/count) => 38)
  (fact "On fill-na"
    (-> df-50 (g/fill-na -999.0) (g/collect-col "BuildingArea") set)
    => #(% -999.0)
    (-> df-50 (g/fill-na -999.0 ["Regionname"]) (g/collect-col "BuildingArea") set)
    => #(nil? (% -999.0)))
  (fact "On replace"
    (-> df-50 (g/replace "Rooms" {1 -999}) (g/collect-col "Rooms") set)
    => #(% -999)))

(fact "On agg methods" :slow
  (let [grouped (-> df-50 (g/group-by "SellerG"))]
    (-> grouped (g/mean "Price" "Rooms") g/column-names)
    => ["SellerG" "avg(Price)" "avg(Rooms)"]
    (-> grouped (g/min "Price" "Rooms") g/column-names)
    => ["SellerG" "min(Price)" "min(Rooms)"]
    (-> grouped (g/max "Price" "Rooms") g/column-names)
    => ["SellerG" "max(Price)" "max(Rooms)"]
    (-> grouped (g/sum "Price" "Rooms") g/column-names)
    => ["SellerG" "sum(Price)" "sum(Rooms)"]
    (-> grouped g/count g/column-names) => ["SellerG" "count"]))

(fact "On approx-quantile" :slow
  (-> melbourne-df
      (g/approx-quantile "Price" [0.1 0.9] 0.2)) => #(< (first %) (second %))
  (-> melbourne-df
      (g/approx-quantile ["Price"] [0.1 0.9] 0.2))
  => #(< (ffirst %) (second (first %))))

(fact "On random-split" :slow
  (let [[train-df val-df] (-> df-50 (g/random-split [90 10]))]
    (< (g/count val-df)
       (g/count train-df)) => true)
  (let [[train-df val-df] (-> df-50 (g/random-split [90 10] 123))]
    (< (g/count val-df)
       (g/count train-df)) => true))

(facts "On printing functions"
  (fact "should return nil"
    (let [n-lines   #(-> % clojure.string/split-lines count)
          df        (g/select melbourne-df "Suburb" "Address")
          n-columns (-> df g/column-names count)]
      (n-lines (with-out-str (g/show (g/limit df 3)))) => 7
      (n-lines (with-out-str (g/show df {:num-rows 3 :vertical true}))) => 10
      (n-lines (with-out-str (g/show-vertical (g/limit df 3)))) => 9
      (n-lines (with-out-str (g/show-vertical df {:num-rows 3}))) => 10
      (n-lines (with-out-str (g/print-schema df))) => (inc n-columns)
      (n-lines (interop/with-scala-out-str (g/explain df))) => #(< 1 %)
      (n-lines (interop/with-scala-out-str (g/explain df true))) => #(< 10 %))))

(fact "On dtypes"
  (-> melbourne-df g/dtypes :Suburb) => "StringType")

(fact "On local"
  (-> melbourne-df g/local?) => boolean?)

(fact "On ungrouped methods"
  (-> melbourne-df g/spark-session) => (partial instance? SparkSession)
  (-> melbourne-df g/sql-context) => (partial instance? SQLContext))

(facts "On pivot" :slow
  (fact "pivot should return the expected cols"
    (let [pivotted (-> df-20
                       (g/group-by "SellerG")
                       (g/pivot "Method")
                       (g/agg (-> (g/count "*") (g/as "n"))))]
      (-> pivotted g/column-names set) => #{"SellerG" "PI" "S" "SP" "VB"}))
  (fact "pivot should be able to specify pivot columns"
    (let [pivotted (-> df-20
                       (g/group-by "SellerG")
                       (g/pivot "Method" ["SP" "VB" "XYZ"])
                       (g/agg (-> (g/count "*") (g/as "n"))))]
      (-> pivotted g/column-names set) => #{"SellerG" "SP" "VB" "XYZ"})))

(facts "On when"
  (fact "when null and coalesce should be equivalent"
    (-> df-20
        (g/with-column "x"
          (g/when (g/null? "BuildingArea") -999 "BuildingArea"))
        (g/with-column "y"
          (g/coalesce "BuildingArea" -999))
        (g/select (g/=== "x" "y"))
        g/collect-vals
        flatten) => #(every? identity %)))

(facts "On select"
  (fact "should drop unselected columns"
    (-> melbourne-df
        (g/select "Type" "Price")
        g/column-names) => ["Type" "Price"])
  (fact "select-expr works as expected"
    (-> melbourne-df
        (g/select-expr "Price+1" "Rooms-1")
        g/column-names) => ["(Price + 1)" "(Rooms - 1)"]))

(facts "On filter"
  (let [df (-> df-20 (g/select "SellerG"))]
    (fact "should correctly filter rows"
      (-> df
          (g/filter (g/=== "SellerG" (g/lit "Biggin")))
          g/distinct
          g/collect) => [{:SellerG "Biggin"}])
    (fact "should filter correctly with isin"
      (-> df
          (g/filter (g/isin "SellerG" ["Greg" "Collins" "Biggin"]))
          g/distinct
          g/collect-vals
          flatten
          set) => #{"Greg" "Collins" "Biggin"}
      (-> df
          (g/filter (g/not (g/isin "SellerG" ["Greg" "Collins" "Biggin"])))
          g/distinct
          g/collect-vals
          flatten
          set) => #(empty? (clojure.set/intersection % #{"Greg" "Collins" "Biggin"})))
    (fact "should correctly remove rows"
      (-> df
          (g/remove (g/=== "SellerG" (g/lit "Biggin")))
          (g/collect-col "SellerG")
          distinct
          set) => #{"Nelson" "Jellis" "Greg" "LITTLE" "Collins"})))

(facts "On rename-columns"
  (fact "the new name should exist and the old name should not"
    (let [col-names (-> melbourne-df
                        (g/rename-columns {"Regionname" "region_name"})
                        g/column-names
                        set)]
      col-names => #(contains? % "region_name")
      col-names => #(not (contains? % "Regionname"))))
  (fact "with-column-renamed actually renames column"
    (-> df-1
        (g/with-column-renamed "SellerG" "seller")
        g/column-names
        set) => #(nil? (% "SellerG"))))

(facts "On actions" :slow
  (fact "take and take-vals work"
    (count (g/take melbourne-df 5)) => 5
    (count (g/take-vals melbourne-df 10)) => 10)
  (fact "first works"
    (-> melbourne-df (g/select "Method") g/first) => {:Method "S"}
    (-> melbourne-df (g/select "Method") g/first-vals) => ["S"]))

(facts "On drop" :slow
  (fact "dropped columns should no longer exist"
    (let [original-columns (-> melbourne-df g/column-names set)
          columns-to-drop  #{"Suburb" "Price" "YearBuilt"}
          dropped-columns  (-> (apply g/drop melbourne-df columns-to-drop)
                               g/column-names
                               set)]
      (clojure.set/subset? columns-to-drop original-columns) => true
      (clojure.set/intersection columns-to-drop dropped-columns) => empty?))
  (fact "drop duplicates without arg should not drop everything"
    (-> df-20
        (g/select "Method" "SellerG")
        g/drop-duplicates
        g/count) => 10)
  (fact "drop duplicates can be called with columns"
    (-> df-20
        (g/select "Method" "SellerG")
        (g/drop-duplicates "SellerG")
        g/count) => 6))

(facts "On except and intercept" :slow
  (fact "except should exclude the row"
    (-> df-20
        (g/union df-20)
        (g/except df-1)
        g/count) => 19)
  (fact "except all should leave out the duplicates"
    (-> df-20
        (g/union df-20)
        (g/except-all df-1)
        g/count) => 39)
  (fact "except then intercept should be empty"
    (-> df-20
        (g/except df-1)
        (g/intersect df-1)
        g/empty?) => true)
  (fact "intersect all should preserve duplicates"
    (-> df-20
        (g/union df-20)
        (g/intersect-all df-1)
        g/count) => 1)) ; TODO: this should be 2

(facts "On union" :slow
  (fact "Union should double the rows preserve distinctness"
    (let [unioned (g/union df-20 df-20 df-20)]
      (g/count unioned) => 60
      (-> unioned g/distinct g/count) => 20))
  (fact "Union by name should line up the names"
    (let [left (-> df-1 (g/select "Suburb" "SellerG"))
          right (-> df-1 (g/select "SellerG" "Suburb"))]
      (-> left (g/union-by-name right right) g/distinct g/count)) => 1))

(facts "On describe" :slow
  (fact "describe should have the right shape"
    (let [summary (-> df-20 (g/describe "Price"))]
      (g/column-names summary) => ["summary" "Price"]
      (map :summary (g/collect summary)) => ["count" "mean" "stddev" "min" "max"]))
  (fact "summary should only pick some stats"
    (-> df-20
        (g/select "Rooms")
        (g/summary "count" "min")
        g/collect-vals) => [["count" "20"] ["min" "1"]]))

(facts "On sample" :slow
  (let [with-rep    (g/sample df-50 0.8 true)
        without-rep (g/sample df-50 0.8)]
    (fact "Sampling without replacement should have all unique rows"
      (-> without-rep g/distinct g/count) => (g/count without-rep))
    (fact "Sampling with replacement should have less unique rows"
      (-> with-rep g/distinct g/count) => #(< % 40))))

(facts "On order-by" :slow
  (let [df (-> df-20 (g/select (g/as (g/->date-col "Date" "dd/MM/yyyy") "Date")))]
    (fact "should correctly order dates"
      (let [records (-> df (g/order-by (g/desc "Date")) g/collect)
            dates   (map #(str (% "Date")) records)]
        (map compare dates (rest dates)) => #(every? (complement neg?) %)))
    (fact "should correctly order dates"
      (let [records (-> df (g/order-by (g/asc "Date")) g/collect)
            dates   (map #(str (% "Date")) records)]
        (map compare dates (rest dates)) => #(every? (complement neg?) %)))))

(facts "On caching" :slow
  (fact "should keeps data in memory")
  (let [df (-> df-1 g/cache)]
    (.. df storageLevel useMemory) => true)
  (let [df (-> df-1 g/persist)]
    (.. df storageLevel useMemory) => true))

(facts "On repartition" :slow
  (fact "able to repartition by a number"
    (-> df-20
        (g/repartition 2)
        g/partitions
        count) => 2)
  (fact "able to repartition by columns"
    (-> df-20
        (g/repartition "Suburb" "SellerG")
        g/partitions
        count) => #(< 1 %))
  (fact "able to repartition by number and columns"
    (-> df-20
        (g/repartition 10 "Suburb" "SellerG")
        g/partitions
        count) => 10)
  (fact "able to repartition by range by columns"
    (-> df-20
        (g/repartition-by-range "Suburb" "SellerG")
        g/partitions
        count) => 7)
  (fact "able to repartition by range by number and columns"
    (-> df-20
        (g/repartition-by-range 3 "Suburb" "SellerG")
        g/partitions
        count) => 3)
  (fact "sort within partitions is differnt to sort"
    (let [sorted  (-> df-20
                      (g/select "Method" "SellerG")
                      (g/order-by "Method")
                      g/collect-vals)
          sorted-within (-> df-20
                            (g/select "Method" "SellerG")
                            (g/repartition 2 "SellerG")
                            (g/sort-within-partitions "Method")
                            g/collect-vals)]
      (= sorted sorted-within) => false
      (set sorted) => (set sorted-within)))
  (fact "coalesce should reduce the number of partitions"
    (-> df-20
        (g/repartition 5)
        (g/coalesce 2)
        g/partitions
        count) => 2))

(facts "On join" :slow
  (fact "normal join works as expected"
    (let [n-listings (-> df-50
                         (g/group-by "Suburb")
                         (g/agg (g/as (g/count "*") "n_listings")))]
      (-> df-50 (g/join n-listings "Suburb") g/column-names set)
      => #(contains? % "n_listings")
      (-> df-50 (g/join n-listings "Suburb" "inner") g/column-names set)
      => #(contains? % "n_listings")
      (-> df-50 (g/join n-listings ["Suburb"] "inner") g/column-names set)
      => #(contains? % "n_listings")))
  (fact "cross-join works as expected"
    (-> df-20
        (g/select "Suburb")
        (g/cross-join (-> df-20 (g/select "Method")))
        g/count) => 400))

(facts "On group-by and agg" :slow
  (fact "should have the right shape"
    (let [agged (-> df-50
                    (g/group-by "Type")
                    (g/agg
                      (-> (g/count "*") (g/as "n_rows"))
                      (-> (g/max "Price") (g/as "max_price"))))]
      (g/count agged) => (-> df-50 (g/select "Type") g/distinct g/count)
      (g/column-names agged) => ["Type" "n_rows" "max_price"]))
  (fact "agg-all should apply to all columns"
    (-> df-20
        (g/select "Price" "Regionname" "Car")
        (g/agg-all g/count-distinct)
        g/collect
        first
        count) => 3)
  (fact "works with nested data structure"
    (let [agged    (-> df-20
                       (g/group-by "SellerG")
                       (g/agg
                         (-> (g/collect-list "Suburb") (g/as "suburbs_list"))
                         (-> (g/collect-set "Suburb") (g/as "suburbs_set"))))
          exploded (g/with-column agged "exploded" (g/explode "suburbs_list"))]
      (g/count agged) => #(< % 20)
      (g/count exploded) => 20)))
