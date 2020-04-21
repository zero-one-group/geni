(ns zero-one.geni.sql-functions-test
  (:require
    [clojure.string]
    [midje.sweet :refer [facts fact =>]]
    [zero-one.geni.core :as g :refer [dataframe]])
  (:import
    (org.apache.spark.sql.expressions WindowSpec)))

(facts "On format number"
  (fact "should format number correctly"
    (-> @dataframe
        (g/limit 1)
        (g/select (g/format-number (g/lit 1234.56789) 2))
        g/collect-vals) => [["1,234.57"]]))

(fact "On arithmetic functions"
  (-> @dataframe
      (g/limit 1)
      (g/select
        (-> (g/* (g/log "Price") (g/lit 0.5)))
        (-> (g// (g/log "Price") (g/lit 2.0)))
        (-> (g/log (g/sqrt "Price")))
        (-> (g/log (g/pow "Price" 0.5))))
      g/collect-vals
      first
      distinct
      count) => 1
  (-> @dataframe
      (g/limit 1)
      (g/select "Price" (-> (g/abs (g/negate "Price"))))
      g/collect-vals
      first
      distinct
      count) => 1
  (-> @dataframe
      (g/limit 1)
      (g/select
        (g/+ (g/lit 1) (g/lit 1)))
      g/collect-vals) => [[2]]
  (-> @dataframe
      (g/limit 1)
      (g/with-column "two" (g/lit 2))
      (g/with-column "three" (g/lit 3))
      (g/select (g/pow "two" "three"))
      g/collect-vals) => [[8.0]])

(facts "On group-by + agg functions"
  (let [n-rows  20
        summary (-> @dataframe
                    (g/limit n-rows)
                    (g/agg
                      (g/count (g/->column "BuildingArea"))
                      (list
                        (g/null-rate "BuildingArea")
                        (g/null-count "BuildingArea"))
                      (g/min "Price")
                      (g/sum "Price")
                      (g/mean "Price")
                      (g/stddev "Price")
                      (g/variance "Price")
                      (g/max "Price"))
                    g/collect
                    first)]
    (fact "common SQL functions should work"
      (summary "avg(Price)") => #(< (summary "min(Price)")
                                    %
                                    (summary "max(Price)"))
      (summary "null_count(BuildingArea)") => (-> (summary "null_rate(BuildingArea)")
                                                  (* 20)
                                                  int)
      (+ (summary "count(BuildingArea)")
         (summary "null_count(BuildingArea)")) => n-rows
      (let [std-dev  (summary "stddev_samp(Price)")
            variance (summary "var_samp(Price)")]
        (Math/abs (- (Math/pow std-dev 2) variance))) => #(< % 1e-6))
    (fact "count and count distinct should be similar"
      (-> @dataframe
          (g/limit 100)
          (g/agg
            (-> (g/count-distinct "SellerG"))
            (-> (g/approx-count-distinct "SellerG")))
          g/collect-vals
          first) => #(< 0.95 (/ (first %) (second %)) 1.05)
      (-> @dataframe
          (g/limit 100)
          (g/agg
            (g/count-distinct "SellerG")
            (g/approx-count-distinct "SellerG" 0.1))
          g/collect-vals
          first) => #(< 0.95 (/ (first %) (second %)) 1.05))))

(facts "On windowing"
  (fact "can instantiate empty WindowSpec"
    (g/window {}) => #(instance? WindowSpec %))
  (let [records    (-> @dataframe
                       (g/limit 10)
                       (g/select
                         "SellerG"
                         (-> (g/max "Price")
                             (g/over (g/window {:partition-by "SellerG"}))
                             (g/- "Price")
                             (g/as "price-gap"))
                         (-> (g/row-number)
                             (g/over (g/window {:partition-by "SellerG"
                                                :order-by (g/desc "Price")}))
                             (g/as "row-num")))
                       (g/filter (g/=== "SellerG" (g/lit "Nelson")))
                       g/collect)
        price-gaps (map #(% "price-gap") records)]
    (map vector price-gaps (rest price-gaps))
    => (fn [pairs] (every? #(< (first %) (second %)) pairs))
    (map #(% "row-num") records) => [1 2 3])
  (fact "count rows last week"
    (-> @dataframe
        (g/limit 10)
        (g/select (-> (g/unix-timestamp "Date" "dd/MM/yyyy") (g/as "date")))
        (g/select
          (-> (g/count "*")
              (g/over (g/window {:partition-by "date"
                                 :order-by "date"
                                 :range-between [(* -7 60 60 24) 0]}))))
        g/collect-vals
        flatten
        set) => #{1 2 3})
  (fact "count rows in the last two rows"
    (-> @dataframe
        (g/limit 10)
        (g/select (-> (g/unix-timestamp "Date" "dd/MM/yyyy") (g/as "date")))
        (g/select
          (-> (g/count "*")
              (g/over (g/window {:partition-by "date"
                                 :order-by "date"
                                 :rows-between [0 1]}))))
        g/collect-vals
        flatten
        set) => #{1 2}))

(facts "On time functions"
  (fact "correct time comparisons"
    (-> @dataframe
        (g/limit 3)
        (g/select
          (-> (g/unix-timestamp) (g/as "now"))
          (-> (g/unix-timestamp (g/lit "2020/04/17") "yyyy/MM/dd") (g/as "past"))
          (-> (g/unix-timestamp (g/to-date (g/lit "9999/12/31") "yyyy/MM/dd")) (g/as "future")))
        (g/select
          (-> (g/< "now" "future"))
          (-> (g/<= "now" "future"))
          (-> (g/<= "future" "future"))
          (-> (g/> "now" "past"))
          (-> (g/>= "now" "past"))
          (-> (g/>= "past" "past")))
        g/collect-vals) => #(every? identity (flatten %)))
  (fact "correct time extraction"
    (let [date (g/lit "1930-12-30 13:15:05")]
      (-> @dataframe
          (g/limit 1)
          (g/select
            (-> (g/year date) (g/as "year"))
            (-> (g/month date) (g/as "month"))
            (-> (g/day-of-month date) (g/as "day-of-month"))
            (-> (g/day-of-week date) (g/as "day-of-week"))
            (-> (g/day-of-year date) (g/as "day-of-year"))
            (-> (g/hour date) (g/as "hour"))
            (-> (g/minute date) (g/as "minute"))
            (-> (g/second date) (g/as "second")))
          g/collect
          first) => {"day-of-month" 30
                     "day-of-week" 3
                     "day-of-year" 364
                     "hour" 13
                     "minute" 15
                     "month" 12
                     "second" 5
                     "year" 1930})))

(fact "hashing should give unique rows"
  (let [df        (g/limit @dataframe 10)
        n-sellers (-> df (g/select "SellerG") g/distinct g/count)]
    (-> df (g/select (g/md5 "SellerG")) g/distinct g/count) => n-sellers
    (-> df (g/select (g/sha1 "SellerG")) g/distinct g/count) => n-sellers
    (-> df (g/select (g/sha2 "SellerG" 256)) g/distinct g/count) => n-sellers))

(fact "correct substring"
  (-> @g/dataframe
      (g/limit 10)
      (g/select (g/substring "Suburb" 3 4))
      g/distinct
      g/collect-vals) => [["bots"]])
