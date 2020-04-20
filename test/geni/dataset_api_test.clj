(ns geni.dataset-api-test
  (:require
    [clojure.set]
    [clojure.string]
    [geni.core :as g :refer [dataframe]]
    [midje.sweet :refer [facts fact =>]]))

(facts "On pivot"
  (fact "pivot should return the expected cols"
    (let [pivotted (-> @dataframe
                       (g/limit 20)
                       (g/group-by "SellerG")
                       (g/pivot "Method")
                       (g/agg (-> (g/count "*") (g/as "n"))))]
      (-> pivotted g/column-names set) => #{"SellerG" "PI" "S" "SP" "VB"}))
  (fact "pivot should be able to specify pivot columns"
    (let [pivotted (-> @dataframe
                       (g/limit 20)
                       (g/group-by "SellerG")
                       (g/pivot "Method" ["SP" "VB" "XYZ"])
                       (g/agg (-> (g/count "*") (g/as "n"))))]
      (-> pivotted g/column-names set) => #{"SellerG" "SP" "VB" "XYZ"})))

(facts "On when"
  (fact "when null and coalesce should be equivalent"
    (-> @dataframe
        (g/limit 20)
        (g/with-column "x"
          (g/when (g/null? "BuildingArea") (g/lit -999) "BuildingArea"))
        (g/with-column "y"
          (g/coalesce "BuildingArea" (g/lit -999)))
        (g/select (g/=== "x" "y"))
        g/collect-vals) => #(every? identity (flatten %))))

(facts "On actions"
  (fact "take and take-vals work"
    (count (g/take @dataframe 5)) => 5
    (count (g/take-vals @dataframe 10)) => 10))

(facts "On drop"
  (fact "dropped columns should no longer exist"
    (let [original-columns (-> @dataframe g/column-names set)
          columns-to-drop  #{"Suburb" "Price" "YearBuilt"}
          dropped-columns  (-> (apply g/drop @dataframe columns-to-drop)
                               g/column-names
                               set)]
      (clojure.set/subset? columns-to-drop original-columns) => true
      (clojure.set/intersection columns-to-drop dropped-columns) => empty?)))

(facts "On union"
  (fact "Union should double the rows preserve distinctness"
    (let [df      (-> @dataframe (g/limit 10))
          unioned (g/union df df)]
      (g/count unioned) => 20
      (-> unioned g/distinct g/count) => 10)))

(facts "On sample"
  (let [df          (-> @dataframe (g/limit 50))
        with-rep    (g/sample df 0.8 true)
        without-rep (g/sample df 0.8)]
    (fact "Sampling without replacement should have all unique rows"
      (-> without-rep g/distinct g/count) => (g/count without-rep))
    (fact "Sampling with replacement should have less unique rows"
      (-> with-rep g/distinct g/count) => #(< % 40))))

(facts "On printing functions"
  (fact "should return nil"
    (let [n-lines   #(-> % clojure.string/split-lines count)
          df        (g/select @dataframe "Suburb" "Address")
          n-columns (-> df g/column-names count)]
      (n-lines (with-out-str (g/show (g/limit df 3)))) => 7
      (n-lines (with-out-str (g/show df {:num-rows 3 :vertical true}))) => 10
      (n-lines (with-out-str (g/show-vertical (g/limit df 3)))) => 9
      (n-lines (with-out-str (g/show-vertical df {:num-rows 3}))) => 10
      (n-lines (with-out-str (g/print-schema df))) => (inc n-columns))))

(facts "On select"
  (fact "should drop unselected columns"
    (-> @dataframe
        (g/select "Type" "Price")
        g/column-names) => ["Type" "Price"]))

(facts "On join"
  (let [df         (-> @dataframe (g/limit 30))
        n-listings (-> df
                     (g/group-by "Suburb")
                     (g/agg (g/as (g/count "*") "n_listings")))]
    (-> df (g/join n-listings "Suburb") g/column-names set)
    => #(contains? % "n_listings")
    (-> df (g/join n-listings "Suburb" "inner") g/column-names set)
    => #(contains? % "n_listings")
    (-> df (g/join n-listings ["Suburb"] "inner") g/column-names set)
    => #(contains? % "n_listings")))

(facts "On filter"
  (let [df (-> @dataframe (g/limit 20) (g/select "SellerG"))]
    (fact "should correctly filter rows"
      (-> df
          (g/filter (g/=== "SellerG" (g/lit "Biggin")))
          g/distinct
          g/collect) => [{"SellerG" "Biggin"}])
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
          set) => #(empty? (clojure.set/intersection % #{"Greg" "Collins" "Biggin"})))))

(facts "On order-by"
  (let [df (-> @dataframe
              (g/limit 10)
              (g/select (g/as (g/->date-col "Date" "dd/MM/yyyy") "Date")))]
    (fact "should correctly order dates"
      (let [records (-> df (g/order-by (g/desc "Date")) g/collect)
            dates   (map #(str (% "Date")) records)]
        (map compare dates (rest dates))
        => (fn [comparisons] (every? #(<= 0 %) comparisons))))
    (fact "should correctly order dates"
      (let [records (-> df (g/order-by (g/asc "Date")) g/collect)
            dates   (map #(str (% "Date")) records)]
        (map compare dates (rest dates))
        => (fn [comparisons] (every? #(<= % 0) comparisons))))))

(facts "On rename-columns"
  (fact "the new name should exist and the old name should not"
    (let [col-names (-> @dataframe
                        (g/rename-columns {"Regionname" "region_name"})
                        g/column-names
                        set)]
      col-names => #(contains? % "region_name")
      col-names => #(not (contains? % "Regionname")))))

(facts "On caching"
  (fact "should keeps data in memory")
  (let [df (-> @dataframe (g/limit 2) g/cache)]
    (.. df storageLevel useMemory) => true))

(facts "On describe"
  (fact "should have the right shape"
    (let [summary (-> @dataframe (g/limit 10) (g/describe "Price"))]
      (g/column-names summary) => ["summary" "Price"]
      (map #(% "summary") (g/collect summary)) => ["count" "mean" "stddev" "min" "max"])))

(facts "On group-by and agg"
  (fact "should have the right shape"
    (let [df    (g/limit @dataframe 30)
          agged (-> df
                    (g/group-by "Type")
                    (g/agg
                      (-> (g/count "*") (g/as "n_rows"))
                      (-> (g/max "Price") (g/as "max_price"))))]
      (g/count agged) => (-> df (g/select "Type") g/distinct g/count)
      (g/column-names agged) => ["Type" "n_rows" "max_price"]))
  (fact "agg-all should apply to all columns"
    (-> @dataframe
        (g/limit 3)
        (g/select "Price" "Regionname" "Car")
        (g/agg-all g/count-distinct)
        g/collect
        first
        count) => 3)
  (fact "works with nested data structure"
    (let [agged    (-> @dataframe
                       (g/limit 50)
                       (g/group-by "SellerG")
                       (g/agg
                         (-> (g/collect-list "Suburb") (g/as "suburbs_list"))
                         (-> (g/collect-set "Suburb") (g/as "suburbs_set"))))
          exploded (g/with-column agged "exploded" (g/explode "suburbs_list"))]
      (g/count agged) => #(< % 50)
      (g/count exploded) => 50)))

