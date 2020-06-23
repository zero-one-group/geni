(ns zero-one.geni.sql-functions-test
  (:require
    [clojure.string]
    [midje.sweet :refer [facts fact =>]]
    [zero-one.geni.core :as g]
    [zero-one.geni.test-resources :refer [melbourne-df df-1 df-20 df-50]])
  (:import
    (org.apache.spark.sql Dataset)
    (org.apache.spark.sql.expressions WindowSpec)))

(fact "On clojure idioms"
  (-> df-1
      (g/select
        (g/inc 1)
        (g/dec 1)
        (g/= 1 1)
        (g/zero? 1)
        (g/pos? 1)
        (g/neg? 1)
        (g/even? 1)
        (g/odd? 1))
      g/collect-vals) => [[2 0 true false true false false true]]
  (-> df-1
      (g/select
        {:a (g/short 1.0)
         :b (g/int 1.0)
         :c (g/long 1.0)
         :d (g/float 1)
         :e (g/double 1)
         :f (g/boolean 1)
         :g (g/byte 1)})
      g/dtypes) => {:a "ShortType"
                    :b "IntegerType"
                    :c "LongType"
                    :d "FloatType"
                    :e "DoubleType"
                    :f "BooleanType"
                    :g "ByteType"})

(fact "On misc functions"
  (-> df-1
      (g/select (g/input-file-name))
      g/collect-vals) => [[""]]
  (-> df-1
      (g/select (g/crc32 (g/encode (g/lit "123") "UTF-8")))
      g/collect-vals
      ffirst) => int?)

(fact "On number functions"
  (-> df-1
      (g/select
        (g/shift-right 2 1)
        (g/shift-left 2 1)
        (g/shift-right-unsigned -2 1)
        (g/bitwise-not -123)
        (g/bround -123.456))
      g/collect-vals) => [[1 4 9223372036854775807 122 -123.0]]
  (-> df-1
      (g/select
        (g/rint 3.2)
        (g/log1p 0)
        (g/log2 2)
        (g/signum -321)
        (g/nanvl 3 2)
        (g/unhex (g/hex 1))
        (g/hypot 3 4)
        (g/base64 (g/unbase64 (g/lit "abc"))))
      g/collect-vals) => [[3.0 0.0 1.0 -1.0 3.0 [1] 5.0 "abc="]]
  (-> df-1
      (g/select
        (g/bin (g/lit "12"))
        (g/conv (g/lit "12") 10 3)
        (g/degrees Math/PI)
        (g/factorial 10)
        (g/radians (/ 180.0 Math/PI))
        (g/greatest 1 2 3)
        (g/least 1 2 3)
        (g/pmod 10 -3))
      g/collect-vals) => [["1100" "110" 180.0 3628800 1.0 3 1 1]])

(fact "On sorting functions" :slow
  (-> df-20
      (g/order-by (g/asc-nulls-first :BuildingArea))
      (g/collect-col :BuildingArea)
      first) => nil?
  (-> df-20
      (g/order-by (g/asc-nulls-last :BuildingArea))
      (g/collect-col :BuildingArea)
      last) => nil?
  (-> df-20
      (g/order-by (g/desc-nulls-first :BuildingArea))
      (g/collect-col :BuildingArea)
      first) => nil?
  (-> df-20
      (g/order-by (g/desc-nulls-last :BuildingArea))
      (g/collect-col :BuildingArea)
      last) => nil?)

(facts "On string functions" ;:slow
  (fact "correct ascii"
    (-> df-1
        (g/select
          (g/ascii :Suburb)
          (g/length :Suburb)
          (g/levenshtein :Suburb :Regionname)
          (g/locate "bar" (g/lit "foobar"))
          (g/translate (g/lit "foobar") "bar" "baz")
          (g/initcap (g/lit "abc"))
          (g/instr (g/lit "abcdef") "c")
          (g/decode (g/encode (g/lit "1122") "UTF-8") "UTF-8")
          (g/overlay :Suburb (g/lit "abc") 3)
          (g/overlay :Suburb (g/lit "xyz") 3 1))
        g/collect-vals
        first) => [65 10 19 4 "foobaz" "Abc" 3 "1122" "Ababcsford" "Abxyzotsford"])
  (fact "correct concat-ws"
    (-> df-20
        (g/group-by :Suburb)
        (g/agg (-> (g/collect-set :SellerG) (g/as :sellers)))
        (g/select (g/concat-ws "," :sellers))
        g/collect-vals
        ffirst) => "Biggin,Jellis,Collins,Nelson,Greg,LITTLE")
  (fact "correct substring"
    (-> df-20
        (g/select
          (g/substring :Suburb 3 4)
          (g/substring-index :Suburb "bb" 1)
          (g/substring-index :Suburb "bb" -1)
          (g/soundex :Suburb))
        g/distinct
        g/collect-vals) => [["bots" "A" "otsford" "A132"]]))

(facts "On agg functions"
  (-> df-20
      (g/cube :SellerG :Regionname)
      (g/agg (g/grouping-id :SellerG :Regionname))
      g/first-vals) => ["Biggin" "Northern Metropolitan" 0]
  (-> df-20
      (g/group-by :SellerG)
      (g/agg (-> (g/collect-list :Regionname) (g/as :regions)))
      (g/select (g/posexplode :regions))
      g/count) => 20
  (-> df-20
      (g/group-by :SellerG)
      (g/agg
        (g/first :Regionname)
        (g/last :Regionname))
      g/collect-vals
      first) => ["Biggin" "Northern Metropolitan" "Northern Metropolitan"]
  (-> df-20
      (g/select
        (g/corr :Price :Rooms)
        (g/covar :Price :Rooms)
        (g/covar-pop :Price :Rooms)
        (g/var-pop :Rooms)
        (g/stddev-pop :Price)
        (g/sum-distinct :Rooms))
      g/collect-vals
      flatten) => #(and (= 6 (count %)) (double? (first %))))

(facts "On hash"
  (-> df-20
      (g/select (g/hash :SellerG :Regionname))
      g/collect-vals
      flatten) => #(and (= 6 (count (distinct %)))
                        (= 20 (count %))))

(facts "On expr"
  (-> df-1
      (g/select (g/expr "1"))
      g/collect-vals) => [[1]])

(facts "On column methods" :slow
  (fact "rlike should filter correctly"
    (let [includes-east-or-north? #(or (clojure.string/includes? % "East")
                                       (clojure.string/includes? % "North"))]
      (-> melbourne-df
          (g/filter (g/rlike :Suburb ".(East|North)"))
          (g/select :Suburb)
          g/distinct
          (g/collect-col :Suburb)) => #(every? includes-east-or-north? %)))
  (fact "like should filter correctly"
    (let [includes-south? #(clojure.string/includes? % "South")]
      (-> melbourne-df
          (g/filter (g/like :Suburb "%South%"))
          (g/select :Suburb)
          g/distinct
          (g/collect-col :Suburb)) => #(every? includes-south? %)))
  (fact "contains should filter correctly"
    (let [includes-west? #(clojure.string/includes? % "West")]
      (-> melbourne-df
          (g/filter (g/contains :Suburb "West"))
          (g/select :Suburb)
          g/distinct
          (g/collect-col :Suburb)) => #(every? includes-west? %)))
  (fact "starts-with should filter correctly"
    (-> melbourne-df
        (g/filter (g/starts-with :Suburb "East"))
        (g/select :Suburb)
        g/distinct
        (g/collect-col :Suburb)) => ["East Melbourne"])
  (fact "starts-with should filter correctly"
    (let [ends-with-west? #(= (last (clojure.string/split % #" ")) "West")]
      (-> melbourne-df
          (g/filter (g/ends-with :Suburb "West"))
          (g/select :Suburb)
          g/distinct
          (g/collect-col :Suburb)) => #(every? ends-with-west? %))))

(fact "On broadcast"
  (-> melbourne-df g/broadcast) => #(instance? Dataset %))

(fact "On array functions"
  (-> df-20
      (g/select
        (-> (g/monotonically-increasing-id) (g/as "id")))
      (g/collect-col "id")) => (range 20)
  (-> df-1
      (g/with-column :struct (g/struct :SellerG :Rooms))
      (g/select
        :struct)
      g/collect-vals
      first) => [{:SellerG "Biggin" :Rooms 2}]
  (-> df-1
      (g/with-column "xs" (g/array [1 2 1]))
      (g/with-column "ys" (g/array [3 2 1]))
      (g/with-column "zs" (g/array [(g/lit "x") (g/lit "y")]))
      (g/select
        "xs"
        (g/array-contains "xs" 2)
        (g/array-distinct "xs")
        (g/array-except "ys" "xs")
        (g/array-intersect "ys" "xs")
        (g/array-join "zs" ",")
        (g/array-join "zs" "," "-")
        (g/array-position "xs" 2)
        (g/aggregate :xs 0 g/+)
        (g/aggregate :xs 0 g/+ g/sqr)
        (g/exists :xs g/zero?)
        (g/forall :ys #(g/< % 10)))
      g/collect-vals
      first) => [[1 2 1] true [1 2] [3] [2 1] "x,y" "x,y" 2 4 16 false true]
  (-> df-1
      (g/with-column "ys" (g/array [-3 -2 -1]))
      (g/with-column "xs" (g/array [1 2 1]))
      (g/select
        (g/array-remove "xs" 1)
        (g/array-repeat 1 2)
        (g/array-repeat 2 (g/lit (int 3)))
        (g/array-sort "xs")
        (g/arrays-overlap "xs" "xs")
        (g/element-at "xs" (int 2))
        (g/zip-with "ys" "xs" g/+)
        (g/arrays-zip ["ys" "xs"]))
      g/collect-vals
      first) => [[2] [1 1] [2 2 2] [1 1 2] true 2 [-2 0 0] [{:xs 1 :ys -3}
                                                            {:xs 2 :ys -2}
                                                            {:xs 1 :ys -1}]]
  (-> df-1
      (g/with-column "xs" (g/array [4 5 6 1]))
      (g/select
        (g/reverse "xs")
        (g/size "xs")
        (g/slice "xs" 2 1)
        (g/sort-array "xs")
        (g/sort-array "xs" false)
        (g/array-min "xs")
        (g/array-max "xs")
        (g/array-union "xs" "xs")
        (g/transform "xs" g/inc))
      g/collect-vals
      first) => [[1 6 5 4] 4 [5] [1 4 5 6] [6 5 4 1] 1 6 [4 5 6 1] [5 6 7 2]]
  (-> df-1
      (g/select (g/shuffle (g/array (range 10))))
      g/collect-vals
      flatten
      set) => (set (range 10))
  (-> df-1
      (g/select (g/flatten (g/array [(g/array (range 10))])))
      g/collect-vals
      ffirst) => (range 10)
  (-> df-1
      (g/select (-> (g/split :Regionname " ") (g/as :split)))
      (g/collect-col :split)) => [["Northern" "Metropolitan"]]
  (-> df-1
      (g/select (-> (g/sequence 1 3 1) (g/as :range)))
      (g/collect-col :range)) => [[1 2 3]])

(fact "On random functions" :slow
  (-> df-20
      (g/select
        (-> (g/randn 0) (g/as :norm))
        (-> (g/rand 0) (g/as :unif)))
      (g/agg
        (g/round (g/skewness :norm))
        (g/round (g/kurtosis :unif))
        (g/round (g/covar :unif :norm)))
      g/collect-vals) => [[0.0 -1.0 0.0]]
  (-> df-20
      (g/select
        (-> (g/randn) (g/as :norm))
        (-> (g/rand) (g/as :unif)))
      (g/agg
        (g/variance :norm)
        (g/variance :unif))
      g/collect-vals
      flatten) => #(every? pos? %))

(fact "On comparison and boolean functions"
  (-> df-1
      (g/select
        (g/&& {:a true  :b true})
        (g/&& {:a true  :b false})
        (g/|| {:a false :b false})
        (g/|| {:a true  :b false}))
      g/collect-vals) => [[true false false true]]
  (-> df-1
      (g/select
        (g/&&)
        (g/||))
      g/collect-vals) => [[true false]]
  (-> df-1
      (g/select
        (g/< 1)
        (g/< 1 2 3)
        (g/<= 1 1 1)
        (g/> 1 2 3)
        (g/>= 1 0.99 1.01)
        (g/&& true false)
        (g/|| true false))
      g/collect-vals) => [[true true true false false false true]])

(fact "On trig functions"
  (-> df-1
      (g/select (g/atan2 1 2))
      g/collect-vals
      ffirst) => #(< 0.463 % 0.464)
  (-> df-1
      (g/select
        (g/- (g// (g/sin g/pi) (g/cos g/pi)) (g/tan g/pi))
        (g/- (g// g/pi 2)
             (g/acos 1)
             (g/asin 1))
        (g/+ (g/atan 2) (g/atan -2))
        (g/- (g// (g/sinh 1) (g/cosh 1))
             (g/tanh 1))
        (g/+ (-> 3 g/sin g/sqr)
             (-> 3 g/cos g/sqr)
             -1))
      g/collect-vals
      flatten) => (fn [xs] (every? #(< (Math/abs %) 0.001) xs)))

(fact "On partition ID" :slow
  (-> df-20
      (g/repartition 3)
      (g/select (g/spark-partition-id))
      g/collect-vals
      flatten
      distinct
      count) => 3)

(facts "On formatting"
  (fact "should format number correctly"
    (-> df-1
        (g/select (g/format-number 1234.56789 2))
        g/collect-vals) => [["1,234.57"]])
  (fact "should format strings correctly"
    (-> df-1
        (g/select
          (g/format-string "(Rooms=%d, SellerG=%s)" [:Rooms :SellerG])
          (g/concat (g/lower :SellerG) (g/lit "-") (g/upper :Suburb))
          (-> (g/lit "1") (g/lpad 3 "0") (g/rpad 5 "."))
          (-> (g/lit "0") (g/lpad 3 " ") (g/rpad 5 " ") g/ltrim g/rtrim)
          (-> (g/lit "x") (g/lpad 3 "_") (g/rpad 5 "_") (g/trim "_"))
          (-> (g/lit "abcdefghi") (g/regexp-replace (g/lit "fgh") (g/lit "XYZ")))
          (-> :Regionname (g/regexp-extract "(.*) (.*)" 2)))
        g/collect-vals) => [["(Rooms=2, SellerG=Biggin)"
                             "biggin-ABBOTSFORD"
                             "001.."
                             "0"
                             "x"
                             "abcdeXYZi"
                             "Metropolitan"]]))

(fact "On arithmetic functions"
  (-> df-1
      (g/select
        (-> (g/+ {:a 6 :b 2}))
        (-> (g/- {:a 6 :b 2}))
        (-> (g/* {:a 6 :b 2}))
        (-> (g// {:a 6 :b 2})))
      g/collect-vals) => [[8 4 12 3.0]]
  (-> df-1
      (g/select
        (-> (g/mod 19 7))
        (-> (g/between 1 0 2))
        (-> (g/between -2 -1 0))
        (-> (g/nan? 0))
        (-> (g/cbrt 27)))
      g/collect-vals) => [[5 true false false 3.0]]
  (-> df-1
      (g/select
        (-> (g/* (g/log :Price) 0.5))
        (-> (g// (g/log :Price) 2.0))
        (-> (g/log (g/sqrt :Price)))
        (-> (g/log (g/pow :Price 0.5))))
      g/collect-vals
      first
      distinct
      count) => 1
  (-> df-1
      (g/select :Price (-> (g/abs (g/negate :Price))))
      g/collect-vals
      first
      distinct
      count) => 1
  (-> df-1 (g/select (g/+ 1 1)) g/collect-vals) => [[2]]
  (-> df-1
      (g/with-column "two" 2)
      (g/with-column "three" 3)
      (g/select (g/pow "two" "three"))
      g/collect-vals) => [[8.0]]
  (-> df-1 (g/select (g/+) (g/*)) g/collect-vals) => [[0 1]]
  (-> df-1
      (g/select
        (g/=== (g/ceil 1.23)
               (g/floor 2.34)
               (g/round 2.49)
               (g/round 1.51))
        (g/log (g/exp 1))
        (g/expm1 0)
        (g/log10 10))
      g/collect-vals) => [[true 1.0 0.0 1.0]])

(facts "On group-by + agg functions" :slow
  (let [summary (-> df-20
                    (g/agg
                      (g/count (g/->column :BuildingArea))
                      (list
                        (g/null-rate :BuildingArea)
                        (g/null-count :BuildingArea))
                      (g/min :Price)
                      (g/sum :Price)
                      (g/mean :Price)
                      (g/stddev :Price)
                      (g/variance :Price)
                      (g/max :Price))
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
         (summary (keyword "null_count(BuildingArea)"))) => 20
      (let [std-dev  (summary (keyword "stddev_samp(Price)"))
            variance (summary (keyword "var_samp(Price)"))]
        (Math/abs (- (Math/pow std-dev 2) variance))) => #(< % 1e-6))
    (fact "count distinct and approx count distinct should be similar"
      (-> df-50
          (g/agg
            (-> (g/count-distinct :SellerG))
            (-> (g/approx-count-distinct :SellerG)))
          g/collect-vals
          first) => #(< 0.95 (/ (first %) (second %)) 1.05)
      (-> df-50
          (g/agg
            (g/count-distinct :SellerG)
            (g/approx-count-distinct :SellerG 0.1))
          g/collect-vals
          first) => #(< 0.9 (/ (first %) (second %)) 1.1))
    (fact "count distinct can take a map"
      (-> df-50
          (g/agg
            (g/count-distinct {:seller :SellerG
                               :suburb :Suburb}))
          g/column-names) => ["count(SellerG AS `seller`, Suburb AS `suburb`)"])))

(facts "On window functions" :slow
  (let [window  (g/window {:partition-by :SellerG :order-by :Price})]
    (-> df-20
        (g/select
          (-> (g/cume-dist) (g/over window))
          (-> (g/percent-rank) (g/over window)))
        g/collect-vals) => #(every? double? (flatten %))
    (-> df-20
        (g/select
          (-> (g/rank) (g/over window))
          (-> (g/dense-rank) (g/over window))
          (-> (g/ntile 2) (g/over window)))
        g/collect-vals) => #(every? int? (flatten %))
    (-> df-20
        (g/select
          (-> (g/lag :Price 1) (g/over window))
          (-> (g/lag :Price 1 -999) (g/over window)))
        g/collect-vals) => #(and (nil? (ffirst %))
                                 (= -999.0 (second (first %))))
    (-> df-20
        (g/select
          (-> (g/lead :Price 1) (g/over window))
          (-> (g/lead :Price 1 -999) (g/over window)))
        g/collect-vals) => #(and (nil? (first (last %)))
                                 (= -999.0 (second (last %))))))

(facts "On windowing" :slow
  (fact "can instantiate empty WindowSpec"
    (g/window {}) => #(instance? WindowSpec %))
  (let [records    (-> df-20
                       (g/select
                         :SellerG
                         (-> (g/max :Price)
                             (g/over (g/window {:partition-by :SellerG}))
                             (g/- :Price)
                             (g/as "price-gap"))
                         (-> (g/row-number)
                             (g/over (g/window {:partition-by :SellerG
                                                :order-by (g/desc :Price)}))
                             (g/as "row-num")))
                       (g/filter (g/=== :SellerG (g/lit "Nelson")))
                       g/collect)
        price-gaps (map :price-gap records)]
    (map vector price-gaps (rest price-gaps))
    => (fn [pairs] (every? #(< (first %) (second %)) pairs))
    (map :row-num records) => [1 2 3 4])
  (fact "count rows last week"
    (-> df-20
        (g/select (-> (g/unix-timestamp :Date "d/MM/yyyy") (g/as :date)))
        (g/select
          (-> (g/count "*")
              (g/over (g/window {:partition-by :date
                                 :order-by :date
                                 :range-between {:start (* -7 60 60 24) :end 0}}))))
        g/collect-vals
        flatten
        set) => #{1 2 3})
  (fact "count rows in the last two rows"
    (-> df-20
        (g/select (-> (g/unix-timestamp :Date "d/MM/yyyy") (g/as :date)))
        (g/select
          (-> (g/count "*")
              (g/over (g/window {:partition-by :date
                                 :order-by :date
                                 :rows-between {:start 0 :end 1}}))))
        g/collect-vals
        flatten
        set) => #{1 2}))

(facts "On time functions"
  (fact "correct time bucketisation"
    (let [dataframe (-> df-20
                         (g/with-column :date (g/to-date :Date "d/MM/yyyy")))]
      (-> dataframe
          (g/select (g/time-window :date "7 days"))
          g/distinct
          g/count) => #(<= 10 % 14)
      (-> dataframe
          (g/select (g/time-window :date "7 days" "2 days"))
          g/distinct
          g/count) => #(<= 40 % 44)
      (-> dataframe
          (g/select (g/time-window :date "7 days" "3 days" "2 days"))
          g/distinct
          g/count) => #(<= 28 % 32)))
  (fact "correct time arithmetic"
    (-> df-1
        (g/select
          (-> (g/to-timestamp (g/lit "2020-05-12")))
          (-> (g/to-timestamp (g/lit "2020-05-12") "yyyy-MM-dd"))
          (-> (g/to-date (g/lit "2020-05-12")))
          (-> (g/to-date (g/lit "2020-05-12") "yyyy-MM-dd")))
        g/collect-vals
        first) => (fn [[x0 x1 x2 x3]] (and (instance? java.sql.Timestamp x0)
                                           (instance? java.sql.Timestamp x1)
                                           (instance? java.sql.Date x2)
                                           (instance? java.sql.Date x3)))
    (-> df-1
        (g/select (-> (g/to-utc-timestamp (g/lit "2020-05-12"))))
        g/collect-vals
        ffirst
        .getTime) => #(= (mod % 10000) 0)
    (-> df-1
        (g/select (-> (g/from-unixtime 1)))
        g/collect-vals
        ffirst) => #(.contains % "1970-01-01 ")
    (-> df-1
        (g/select (-> (g/quarter (g/lit "2020-05-12"))))
        g/collect-vals
        ffirst) => 2
    (-> df-1
        (g/select
          (-> (g/date-trunc "YYYY" (g/to-timestamp (g/lit "2020-05-12")))))
        g/collect-vals
        ffirst
        .getTime) => #(= (mod % 10000) 0)
    (-> df-1
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
    (-> df-1
        (g/select
          (g/cast (g/current-timestamp) "string")
          (g/cast (g/current-date) "string"))
        g/collect-vals
        flatten) => #(and (clojure.string/includes? (first %) ":")
                          (not (clojure.string/includes? (second %) ":"))))
  (fact "correct time comparisons"
    (-> df-1
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
      (-> df-1
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

(fact "hashing should give unique rows" :slow
  (let [n-sellers (-> df-20 (g/select :SellerG) g/distinct g/count)]
    (-> df-20 (g/select (g/xxhash64 :SellerG)) g/distinct g/count) => n-sellers
    (-> df-20 (g/select (g/md5 :SellerG)) g/distinct g/count) => n-sellers
    (-> df-20 (g/select (g/sha1 :SellerG)) g/distinct g/count) => n-sellers
    (-> df-20 (g/select (g/sha2 :SellerG 256)) g/distinct g/count) => n-sellers))
