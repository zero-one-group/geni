(ns zero-one.geni.sql-functions-test
  (:require
    [clojure.string]
    [midje.sweet :refer [facts fact =>]]
    [zero-one.geni.core :as g]
    [zero-one.geni.test-resources :refer [melbourne-df]])
  (:import
    (org.apache.spark.sql.expressions WindowSpec)))

(fact "On random functions"
  (-> melbourne-df
      (g/limit 20)
      (g/select
        (-> (g/randn 0) (g/as "norm"))
        (-> (g/rand 0) (g/as "unif")))
      (g/agg
        (g/round (g/skewness "norm"))
        (g/round (g/kurtosis "unif"))
        (g/round (g/covar "unif" "norm")))
      g/collect-vals) => [[0.0 -1.0 0.0]]
  (-> melbourne-df
      (g/limit 10)
      (g/select
        (-> (g/randn) (g/as "norm"))
        (-> (g/rand) (g/as "unif")))
      (g/agg
        (g/variance "norm")
        (g/variance "unif"))
      g/collect-vals
      flatten) => #(every? pos? %))

(fact "On comparison and boolean functions"
  (-> melbourne-df
      (g/limit 1)
      (g/select
        (g/&&)
        (g/||))
      g/collect-vals) => [[true false]]
  (-> melbourne-df
      (g/limit 1)
      (g/select
        (g/< (g/lit 1))
        (g/< (g/lit 1) (g/lit 2) (g/lit 3))
        (g/<= (g/lit 1) (g/lit 1) (g/lit 1))
        (g/> (g/lit 1) (g/lit 2) (g/lit 3))
        (g/>= (g/lit 1) (g/lit 0.99) (g/lit 1.01))
        (g/&& (g/lit true) (g/lit false))
        (g/|| (g/lit true) (g/lit false)))
      g/collect-vals) => [[true true true false false false true]])

(fact "On trig functions"
  (-> melbourne-df
      (g/limit 1)
      (g/select
        (g/- (g// (g/sin g/pi) (g/cos g/pi)) (g/tan g/pi))
        (g/- (g// g/pi (g/lit 2))
             (g/acos (g/lit 1))
             (g/asin (g/lit 1)))
        (g/+ (g/atan (g/lit 2)) (g/atan (g/lit -2)))
        (g/- (g// (g/sinh (g/lit 1)) (g/cosh (g/lit 1)))
             (g/tanh (g/lit 1)))
        (g/+ (-> 3 g/lit g/sin g/sqr)
             (-> 3 g/lit g/cos g/sqr)
             (g/lit -1)))
      g/collect-vals
      flatten) => (fn [xs] (every? #(< (Math/abs %) 0.001) xs)))

(fact "On partition ID"
  (-> melbourne-df
      (g/limit 10)
      (g/repartition 3)
      (g/select (g/spark-partition-id))
      g/collect-vals
      flatten
      distinct
      count) => 3)

(facts "On formatting"
  (fact "should format number correctly"
    (-> melbourne-df
        (g/limit 1)
        (g/select (g/format-number (g/lit 1234.56789) 2))
        g/collect-vals) => [["1,234.57"]])
  (fact "should format strings correctly"
    (-> melbourne-df
        (g/limit 1)
        (g/select
          (g/format-string "(Rooms=%d, SellerG=%s)" ["Rooms" "SellerG"])
          (g/concat (g/lower "SellerG") (g/lit "-") (g/upper "Suburb"))
          (-> (g/lit "1") (g/lpad 3 "0") (g/rpad 5 "."))
          (-> (g/lit "0") (g/lpad 3 " ") (g/rpad 5 " ") g/ltrim g/rtrim)
          (-> (g/lit "x") (g/lpad 3 "_") (g/rpad 5 "_") (g/trim "_"))
          (-> (g/lit "abcdefghi") (g/regexp-replace (g/lit "fgh") (g/lit "XYZ")))
          (-> "Regionname" (g/regexp-extract "(.*) (.*)" 2)))
        g/collect-vals) => [["(Rooms=2, SellerG=Biggin)"
                             "biggin-ABBOTSFORD"
                             "001.."
                             "0"
                             "x"
                             "abcdeXYZi"
                             "Metropolitan"]]))

(fact "On arithmetic functions"
  (-> melbourne-df
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
  (-> melbourne-df
      (g/limit 1)
      (g/select "Price" (-> (g/abs (g/negate "Price"))))
      g/collect-vals
      first
      distinct
      count) => 1
  (-> melbourne-df
      (g/limit 1)
      (g/select
        (g/+ (g/lit 1) (g/lit 1)))
      g/collect-vals) => [[2]]
  (-> melbourne-df
      (g/limit 1)
      (g/with-column "two" (g/lit 2))
      (g/with-column "three" (g/lit 3))
      (g/select (g/pow "two" "three"))
      g/collect-vals) => [[8.0]]
  (-> melbourne-df
      (g/limit 1)
      (g/select (g/+) (g/*))
      g/collect-vals) => [[0 1]]
  (-> melbourne-df
      (g/limit 1)
      (g/select
        (g/=== (g/ceil (g/lit 1.23))
             (g/floor (g/lit 2.34))
             (g/round (g/lit 2.49))
             (g/round (g/lit 1.51)))
        (g/log (g/exp (g/lit 1))))
      g/collect-vals) => [[true 1.0]])

(facts "On group-by + agg functions"
  (let [n-rows  20
        summary (-> melbourne-df
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
      (summary (keyword "avg(Price)"))
      => #(< (summary (keyword "min(Price)"))
             %
             (summary (keyword "max(Price)")))
      (summary (keyword "null_count(BuildingArea)"))
      => (-> (summary (keyword "null_rate(BuildingArea)"))
             (* 20)
             int)
      (+ (summary (keyword "count(BuildingArea)"))
         (summary (keyword "null_count(BuildingArea)"))) => n-rows
      (let [std-dev  (summary (keyword "stddev_samp(Price)"))
            variance (summary (keyword "var_samp(Price)"))]
        (Math/abs (- (Math/pow std-dev 2) variance))) => #(< % 1e-6))
    (fact "count distinct and approx count distinct should be similar"
      (-> melbourne-df
          (g/limit 60)
          (g/agg
            (-> (g/count-distinct "SellerG"))
            (-> (g/approx-count-distinct "SellerG")))
          g/collect-vals
          first) => #(< 0.95 (/ (first %) (second %)) 1.05)
      (-> melbourne-df
          (g/limit 60)
          (g/agg
            (g/count-distinct "SellerG")
            (g/approx-count-distinct "SellerG" 0.1))
          g/collect-vals
          first) => #(< 0.95 (/ (first %) (second %)) 1.05))))

(facts "On windowing"
  (fact "can instantiate empty WindowSpec"
    (g/window {}) => #(instance? WindowSpec %))
  (let [records    (-> melbourne-df
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
        price-gaps (map :price-gap records)]
    (map vector price-gaps (rest price-gaps))
    => (fn [pairs] (every? #(< (first %) (second %)) pairs))
    (map :row-num records) => [1 2 3])
  (fact "count rows last week"
    (-> melbourne-df
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
    (-> melbourne-df
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
  (fact "correct time arithmetic"
    (-> melbourne-df
        (g/limit 1)
        (g/select
          (-> (g/last-day (g/lit "2020-05-12")) (g/cast "string"))
          (-> (g/next-day (g/lit "2020-02-01") "Sunday") (g/cast "string"))
          (-> (g/lit "2020-03-02") (g/date-add 10) (g/date-sub 3) (g/cast "string"))
          (-> (g/date-format (g/lit "2019-02-09") "yyyy~MM~dd") (g/cast "string"))
          (-> (g/lit "2020-02-05") (g/add-months 3) (g/cast "string"))
          (g/week-of-year (g/lit "2020-04-30"))
          (g/round (g/date-diff (g/lit "2020-05-23") (g/lit "2020-04-30")))
          (g/round (g/months-between (g/lit "2020-01-23") (g/lit "2020-04-30"))))
        g/collect-vals) => [["2020-05-31"
                             "2020-02-02"
                             "2020-03-09"
                             "2019~02~09"
                             "2020-05-05"
                             18
                             23
                             -3.0]])
  (fact "correct current times"
    (-> melbourne-df
        (g/limit 1)
        (g/select
          (g/cast (g/current-timestamp) "string")
          (g/cast (g/current-date) "string"))
        g/collect-vals
        flatten) => #(and (clojure.string/includes? (first %) ":")
                          (not (clojure.string/includes? (second %) ":"))))
  (fact "correct time comparisons"
    (-> melbourne-df
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
      (-> melbourne-df
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
          first) => {:day-of-month 30
                     :day-of-week 3
                     :day-of-year 364
                     :hour 13
                     :minute 15
                     :month 12
                     :second 5
                     :year 1930})))

(fact "hashing should give unique rows"
  (let [df        (g/limit melbourne-df 10)
        n-sellers (-> df (g/select "SellerG") g/distinct g/count)]
    (-> df (g/select (g/md5 "SellerG")) g/distinct g/count) => n-sellers
    (-> df (g/select (g/sha1 "SellerG")) g/distinct g/count) => n-sellers
    (-> df (g/select (g/sha2 "SellerG" 256)) g/distinct g/count) => n-sellers))

(fact "correct substring"
  (-> melbourne-df
      (g/limit 10)
      (g/select (g/substring "Suburb" 3 4))
      g/distinct
      g/collect-vals) => [["bots"]])
