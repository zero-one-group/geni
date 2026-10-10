(ns zero-one.geni.sql-functions-test
  (:require
   [clojure.string]
   [clojure.test :refer [deftest is testing]]
   [zero-one.geni.core :as g]
   [zero-one.geni.test-resources :refer [melbourne-df df-1 df-20 df-50]])
  (:import
   (java.sql Timestamp)
   (org.apache.spark.sql Dataset)
   (org.apache.spark.sql.expressions WindowSpec)
   (java.time Instant ZoneId)
   (java.time.format DateTimeFormatter)))

(deftest json-functions-test
  (is (= {:schema-1 "ARRAY<STRUCT<col: BIGINT>>"
          :schema-2 "ARRAY<STRUCT<col: BIGINT>>"
          :from-1   {:a 1 :b 0.8}
          :from-2   {:time (Timestamp. 1440547200000)}
          :from-3   {:a 1}
          :to-1 "{\"a\":1,\"b\":2}"
          :to-2 "{\"time\":\"26/08/2015\"}"
          :to-3 "{\"a\":1,\"b\":null}"}
         (-> (df-1)
             (g/select
              {:schema-1 (g/schema-of-json (g/lit "[{\"col\":0}]"))
               :schema-2 (g/schema-of-json (g/lit "[{\"col\":01}]") {:allowNumericLeadingZeros "true"})
               :from-1   (g/from-json (g/lit "{\"a\":1, \"b\":0.8}") (g/lit "a INT, b DOUBLE"))
               :from-2   (g/from-json (g/lit "{\"time\":\"26/08/2015 00:00:00 GMT\"}")
                                      (g/lit "time Timestamp")
                                      {:timestampFormat "dd/MM/yyyy HH:mm:ss z"})
               :from-3   (g/from-json (g/lit "/* a comment */ {\"a\": 1}") (g/lit "a INT")
                                      {:allowComments true})
               :to-1     (g/to-json (g/struct {:a 1 :b 2}))
               :to-2     (g/to-json (g/struct {:time (g/to-timestamp (g/lit "2015-08-26") "yyyy-MM-dd")})
                                    {:timestampFormat "dd/MM/yyyy"})
               :to-3     (g/to-json (g/struct {:a 1 :b (g/cast (g/lit nil) "int")})
                                    {:ignoreNullFields false})})
             g/collect
             first))))

(deftest csv-functions-test
  (is (= {:schema-1 "STRUCT<_c0: INT, _c1: STRING>"
          :schema-2 "STRUCT<_c0: INT, _c1: STRING>"
          :from-1   {:a 1 :b 0.8}
          :from-2   {:time (Timestamp. 1440547200000)}
          :to-1     "1,2"
          :to-2     "26/08/2015"
          :to-3     "\"1\",\"2\""}
         (-> (df-1)
             (g/select
              {:schema-1 (g/schema-of-csv (g/lit "1,abc"))
               :schema-2 (g/schema-of-csv (g/lit "1|abc") {:delimiter "|"})
               :from-1   (g/from-csv (g/lit "1, 0.8") (g/lit "a INT, b DOUBLE"))
               :from-2   (g/from-csv (g/lit "26/08/2015 00:00:00 GMT")
                                     (g/lit "time Timestamp")
                                     {:timestampFormat "dd/MM/yyyy HH:mm:ss z"})
               :to-1     (g/to-csv (g/struct {:a 1 :b 2}))
               :to-2     (g/to-csv (g/struct {:time (g/to-timestamp (g/lit "2015-08-26") "yyyy-MM-dd")})
                                   {:timestampFormat "dd/MM/yyyy"})
               :to-3     (g/to-csv (g/struct {:a 1 :b 2}) {:quoteAll true})})
             g/collect
             first))))

(deftest map-functions-test
  (is (= [{:location
           {"suburb" "Abbotsford",
            "region" "Northern Metropolitan",
            "council" "Yarra",
            "address" "85 Turner St"},
           :market {"size" 1480000.0, "price" 1480000.0},
           :coord {"lat" -37.7996, "long" 144.9984}}]
         (-> (df-20)
             (g/limit 1)
             (g/select
              {:location (g/map (g/lit "suburb") :Suburb
                                (g/lit "region") :Regionname
                                (g/lit "council") :CouncilArea
                                (g/lit "address") :Address)
               :market   (g/map-from-entries
                          (g/array (g/struct (g/lit "size") (g/double :Price))
                                   (g/struct (g/lit "price") (g/double :Price))))
               :coord    (g/map-from-arrays
                          (g/array (g/lit "lat") (g/lit "long"))
                          (g/array :Lattitude :Longtitude))})
             g/collect)))
  (is (= [{:map {"85 Turner St" {:address "85 Turner St"
                                 :council "Yarra"
                                 :region "Northern Metropolitan"
                                 :suburbs "Abbotsford"}}
           :seller "Biggin"}]
         (-> (df-1)
             (g/with-column :location (g/struct
                                       {:address :Address
                                        :suburbs :Suburb
                                        :region  :Regionname
                                        :council :CouncilArea}))
             (g/group-by :SellerG)
             (g/agg {:keys   (g/collect-list :Address)
                     :values (g/collect-list :location)})
             (g/select {:seller :SellerG
                        :map    (g/map-from-arrays :keys :values)})
             g/collect)))
  (is (= {:arrays  {123 10.0 456 20.0}
          :ass-2   {1 "a" 2 "b" 3 "y" 7 "x"}
          :assoc   {1 "a" 2 "b" 3 "c"}
          :concat  {1 "a" 2 "b" 3 "d" 4 "e"}
          :diss-2  {}
          :dissoc  {3 "d"}
          :entries [{:key 1 :value "a"} {:key 2 :value "b"}]
          :filter  {2 "b"}
          :keys    [1 2]
          :map     {1 "a" 2 "b"}
          :merge   {1 "a" 2 "b" 3 "d" 4 "e" 5 "x"}
          :renamed {10 "a" 20 "b"}
          :select  {4 "e"}
          :update  {3 "d" 4 "E++"}
          :values  ["a" "b"]
          :x-keys  {"1A" "a" "2B" "b"}
          :x-vals  {1 1 2 4}
          :z-2     {1 "a" 2 "b" 3 "d" 4 "e"}
          :zipped  {1 "Aa" 2 "Bb"}}
         (-> (df-1)
             (g/with-column
               :map
               (g/map-from-entries (g/array (g/struct 1 (g/lit "a"))
                                            (g/struct 2 (g/lit "b")))))
             (g/with-column :other (g/map 3 (g/lit "d") 4 (g/lit "e")))
             (g/select
              :map
              {:arrays  (g/map-from-arrays (g/array 123 456) (g/array 10.0 20.0))
               :ass-2   (g/assoc :map 7 (g/lit "x") 3 (g/lit "y"))
               :assoc   (g/assoc :map 3 (g/lit "c"))
               :concat  (g/map-concat :map :other)
               :diss-2  (g/dissoc :other 4 3)
               :dissoc  (g/dissoc :other 4)
               :entries (g/map-entries :map)
               :filter  (g/map-filter :map (fn [k _] (g/even? k)))
               :keys    (g/map-keys :map)
               :merge   (g/merge :map :other (g/map 5 (g/lit "x")))
               :renamed (g/rename-keys :map {1 10 2 20})
               :select  (g/select-keys :other [2 4 1])
               :update  (g/update :other 4 #(g/concat (g/upper %1) %2) (g/lit "++"))
               :values  (g/map-values :map)
               :x-keys  (g/transform-keys :map (fn [k v] (g/concat (g/str k) (g/upper v))))
               :x-vals  (g/transform-values :map (fn [k _] (g/sqr k)))
               :z-2     (g/map-zip-with :map :other (fn [_ v1 v2] (g/coalesce v1 v2)))
               :zipped  (g/map-zip-with :map :map (fn [_ v1 v2] (g/concat (g/upper v2) v1)))})
             g/collect
             first)))
  (is (thrown? IllegalArgumentException (g/assoc :map 7 (g/lit "x") 3))))

(deftest misc-functions-test
  (is (= [[""]]
         (-> (df-1)
             (g/select (g/input-file-name))
             g/collect-vals)))
  (is (int?
       (-> (df-1)
           (g/select (g/crc32 (g/encode (g/lit "123") "UTF-8")))
           g/collect-vals
           ffirst))))

(deftest number-functions-test
  (is (= [[1 4 9223372036854775807 122 -123.0]]
         (-> (df-1)
             (g/select
              (g/shift-right 2 1)
              (g/shift-left 2 1)
              (g/shift-right-unsigned (g/lit -2) 1)
              (g/bitwise-not -123)
              (g/bround -123.456))
             g/collect-vals)))
  (is (= [[3.0 0.0 1.0 -1.0 3.0 [1] 5.0 "abc="]]
         (-> (df-1)
             (g/select
              (g/rint 3.2)
              (g/log1p 0)
              (g/log2 2)
              (g/signum -321)
              (g/nanvl 3 2)
              (g/unhex (g/hex 1))
              (g/hypot 3 4)
              (g/base64 (g/unbase64 (g/lit "abc"))))
             g/collect-vals)))
  (is (= [["1100" "110" 180.0 3628800 1.0 3 1 1]]
         (-> (df-1)
             (g/select
              (g/bin (g/lit "12"))
              (g/conv (g/lit "12") 10 3)
              (g/degrees Math/PI)
              (g/factorial 10)
              (g/radians (/ 180.0 Math/PI))
              (g/greatest 1 2 3)
              (g/least 1 2 3)
              (g/pmod 10 -3))
             g/collect-vals))))

(deftest string-functions-test
  (testing "correct ascii"
    (is (= [65 10 19 4 "foobaz" "Abc" 3 "1122" "Ababcsford" "Abxyzotsford"]
           (-> (df-1)
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
               first))))
  (testing "correct concat-ws"
    (is (= "Biggin,Collins,Greg,Jellis,LITTLE,Nelson"
           (-> (df-20)
               (g/group-by :Suburb)
               (g/agg (-> (g/collect-set :SellerG) g/array-sort (g/as :sellers)))
               (g/select (g/concat-ws "," :sellers))
               g/collect-vals
               ffirst))))
  (testing "correct substring"
    (is (= [["bots" "A" "otsford" "A132"]]
           (-> (df-20)
               (g/select
                (g/substring :Suburb 3 4)
                (g/substring-index :Suburb "bb" 1)
                (g/substring-index :Suburb "bb" -1)
                (g/soundex :Suburb))
               g/distinct
               g/collect-vals)))))

(deftest agg-functions-test
  (is (= ["Biggin" "Northern Metropolitan" 0]
         (-> (df-20)
             (g/cube :SellerG :Regionname)
             (g/agg (g/grouping-id :SellerG :Regionname))
             g/first-vals)))
  (is (= 20
         (-> (df-20)
             (g/group-by :SellerG)
             (g/agg (-> (g/collect-list :Regionname) (g/as :regions)))
             (g/select (g/posexplode :regions))
             g/count)))
  (is (= ["Biggin" "Northern Metropolitan" "Northern Metropolitan"]
         (-> (df-20)
             (g/group-by :SellerG)
             (g/agg
              (g/first :Regionname)
              (g/last :Regionname))
             g/collect-vals
             first)))
  (let [actual (-> (df-20)
                   (g/select
                    (g/corr :Price :Rooms)
                    (g/covar :Price :Rooms)
                    (g/covar-pop :Price :Rooms)
                    (g/var-pop :Rooms)
                    (g/stddev-pop :Price)
                    (g/sum-distinct :Rooms))
                   g/collect-vals
                   flatten)]
    (is (and (= 6 (count actual)) (double? (first actual))))))

(deftest hash-test
  (let [actual (-> (df-20)
                   (g/select (g/hash :SellerG :Regionname))
                   g/collect-vals
                   flatten)]
    (is (and (= 6 (count (distinct actual)))
             (= 20 (count actual))))))

(deftest expr-test
  (is (= [[1]]
         (-> (df-1)
             (g/select (g/expr "1"))
             g/collect-vals))))

(deftest broadcast-test
  (is (instance? Dataset (-> (melbourne-df) g/broadcast))))

(deftest array-functions-test
  (is (= (range 20)
         (-> (df-20)
             (g/select
              (-> (g/monotonically-increasing-id) (g/as "id")))
             (g/collect-col "id"))))
  (is (= {:struct     {:SellerG "Biggin" :Rooms 2}
          :filtered-1 [2]
          :filtered-2 [-1 0 1]}
         (-> (df-1)
             (g/select
              {:struct (g/struct :SellerG :Rooms)
               :filtered-1 (g/filter (g/array 1 2 3) g/even?)
               :filtered-2 (g/filter (g/array -1 0 1 2 3) #(g/< (g/+ %1 %2) 4))})
             g/collect
             first)))
  (is (= [[1 2 1] true [1 2] [3] [2 1] "x,y" "x,y" 2 4 16 false true]
         (-> (df-1)
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
             first)))
  (is (= [[2] [1 1] [2 2 2] [1 1 2] true 2 [-2 0 0] [{:xs 1 :ys -3}
                                                     {:xs 2 :ys -2}
                                                     {:xs 1 :ys -1}]]
         (-> (df-1)
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
             first)))
  (is (= [[1 6 5 4] 4 [5] [1 4 5 6] [6 5 4 1] 1 6 [4 5 6 1] [5 6 7 2]]
         (-> (df-1)
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
             first)))
  (is (= (set (range 10))
         (-> (df-1)
             (g/select (g/shuffle (g/array (range 10))))
             g/collect-vals
             flatten
             set)))
  (is (= (range 10)
         (-> (df-1)
             (g/select (g/flatten (g/array [(g/array (range 10))])))
             g/collect-vals
             ffirst)))
  (is (= [["Northern" "Metropolitan"]]
         (-> (df-1)
             (g/select (-> (g/split :Regionname " ") (g/as :split)))
             (g/collect-col :split))))
  (is (= [[1 2 3]]
         (-> (df-1)
             (g/select (-> (g/sequence 1 3 1) (g/as :range)))
             (g/collect-col :range)))))

(deftest random-functions-test
  (is (= [[0.0 -1.0 0.0]]
         (-> (df-20)
             (g/select
              (-> (g/randn 0) (g/as :norm))
              (-> (g/rand 0) (g/as :unif)))
             (g/agg
              (g/round (g/skewness :norm))
              (g/round (g/kurtosis :unif))
              (g/round (g/covar :unif :norm)))
             g/collect-vals)))
  (is (every? pos? (-> (df-20)
                       (g/select
                        (-> (g/randn) (g/as :norm))
                        (-> (g/rand) (g/as :unif)))
                       (g/agg
                        (g/variance :norm)
                        (g/variance :unif))
                       g/collect-vals
                       flatten))))

(deftest trig-functions-test
  (is (< 0.463 (-> (df-1)
                   (g/select (g/atan2 1 2))
                   g/collect-vals
                   ffirst) 0.464))
  (let [xs (-> (df-1)
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
               flatten)]
    (is (every? #(< (Math/abs %) 0.001) xs))))

(deftest partition-id-test
  (is (= 3
         (-> (df-20)
             (g/repartition 3)
             (g/select (g/spark-partition-id))
             g/collect-vals
             flatten
             distinct
             count))))

(deftest formatting-test
  (testing "should format number correctly"
    (is (= [["1,234.57"]]
           (-> (df-1)
               (g/select (g/format-number 1234.56789 2))
               g/collect-vals))))
  (testing "should format strings correctly"
    (is (= [["(Rooms=2, SellerG=Biggin)"
             "(Rooms=2, SellerG=Biggin)"
             "biggin-ABBOTSFORD"
             "001.."
             "0"
             "x"
             "abcdeXYZi"
             "Metropolitan"]]
           (-> (df-1)
               (g/select
                (g/format-string "(Rooms=%d, SellerG=%s)" [:Rooms :SellerG])
                (g/format-string "(Rooms=%d, SellerG=%s)" :Rooms :SellerG)
                (g/concat (g/lower :SellerG) (g/lit "-") (g/upper :Suburb))
                (-> (g/lit "1") (g/lpad 3 "0") (g/rpad 5 "."))
                (-> (g/lit "0") (g/lpad 3 " ") (g/rpad 5 " ") g/ltrim g/rtrim)
                (-> (g/lit "x") (g/lpad 3 "_") (g/rpad 5 "_") (g/trim "_"))
                (-> (g/lit "abcdefghi") (g/regexp-replace (g/lit "fgh") (g/lit "XYZ")))
                (-> :Regionname (g/regexp-extract "(.*) (.*)" 2)))
               g/collect-vals))))
  (testing "should trim spaces, or any of the given characters"
    (is (= [["x  " "  x" "x" "x-_" "_-x" "x"]]
           (-> (df-1)
               (g/select
                (g/ltrim (g/lit "  x  "))
                (g/rtrim (g/lit "  x  "))
                (g/trim (g/lit "  x  "))
                (g/ltrim (g/lit "_-x-_") "-_")
                (g/rtrim (g/lit "_-x-_") "-_")
                (g/trim (g/lit "_-x-_") "-_"))
               g/collect-vals)))))

(deftest arithmetic-functions-test
  (is (= [[8 4 12 3.0]]
         (-> (df-1)
             (g/select
              (-> (g/+ {:a 6 :b 2}))
              (-> (g/- {:a 6 :b 2}))
              (-> (g/* {:a 6 :b 2}))
              (-> (g// {:a 6 :b 2})))
             g/collect-vals)))
  (is (= [[5 true false false 3.0]]
         (-> (df-1)
             (g/select
              (-> (g/mod 19 7))
              (-> (g/between 1 0 2))
              (-> (g/between -2 -1 0))
              (-> (g/nan? 0))
              (-> (g/cbrt 27)))
             g/collect-vals)))
  (is (= 1
         (-> (df-1)
             (g/select
              (-> (g/* (g/log :Price) 0.5))
              (-> (g// (g/log :Price) 2.0))
              (-> (g/log (g/sqrt :Price)))
              (-> (g/log (g/pow :Price 0.5))))
             g/collect-vals
             first
             distinct
             count)))
  (is (= 1
         (-> (df-1)
             (g/select :Price (-> (g/abs (g/negate :Price))))
             g/collect-vals
             first
             distinct
             count)))
  (is (= [[2]] (-> (df-1) (g/select (g/+ 1 1)) g/collect-vals)))
  (is (= [[8.0]]
         (-> (df-1)
             (g/with-column "two" 2)
             (g/with-column "three" 3)
             (g/select (g/pow "two" "three"))
             g/collect-vals)))
  (is (= [[0 1]] (-> (df-1) (g/select (g/+) (g/*)) g/collect-vals)))
  (is (= [[true 1.0 0.0 1.0]]
         (-> (df-1)
             (g/select
              (g/=== (g/ceil 1.23)
                     (g/floor 2.34)
                     (g/round 2.49)
                     (g/round 1.51))
              (g/log (g/exp 1))
              (g/expm1 0)
              (g/log10 10))
             g/collect-vals))))

(deftest group-by-agg-functions-test
  (let [summary (-> (df-20)
                    (g/agg
                     (g/count (g/->column :BuildingArea))
                     (list
                      (g/null-rate :BuildingArea)
                      (g/null-count :BuildingArea))
                     (g/min :Price)
                     (g/sum :Price)
                     (g/mean :Price)
                     (g/as (g/stddev :Price) "std-dev")
                     (g/as (g/variance :Price) "variance")
                     (g/max :Price))
                    g/collect
                    first)]
    (testing "common SQL functions should work"
      (is (< (summary (keyword "min(Price)"))
             (summary (keyword "avg(Price)"))
             (summary (keyword "max(Price)"))))
      (is (= (-> (summary (keyword "null_rate(BuildingArea)"))
                 (* 20)
                 int)
             (summary (keyword "null_count(BuildingArea)"))))
      (is (= 20
             (+ (summary (keyword "count(BuildingArea)"))
                (summary (keyword "null_count(BuildingArea)")))))
      (is (< (let [std-dev  (summary :std-dev)
                   variance (summary :variance)]
               (Math/abs (- (Math/pow std-dev 2) variance))) 1e-6)))
    (testing "count distinct and approx count distinct should be similar"
      (let [actual (-> (df-50)
                       (g/agg
                        (-> (g/count-distinct :SellerG))
                        (-> (g/approx-count-distinct :SellerG)))
                       g/collect-vals
                       first)]
        (is (< 0.95 (/ (first actual) (second actual)) 1.05)))
      (let [actual (-> (df-50)
                       (g/agg
                        (g/count-distinct :SellerG)
                        (g/approx-count-distinct :SellerG 0.1))
                       g/collect-vals
                       first)]
        (is (< 0.9 (/ (first actual) (second actual)) 1.1))))
    (testing "count distinct can take a map"
      (is (= ["count(DISTINCT SellerG AS seller, Suburb AS suburb)"]
             (-> (df-50)
                 (g/agg
                  (g/count-distinct {:seller :SellerG
                                     :suburb :Suburb}))
                 g/column-names))))))

(deftest window-functions-test
  (let [window  (g/window {:partition-by :SellerG :order-by :Price})]
    (is (every? double? (flatten (-> (df-20)
                                     (g/select
                                      (-> (g/cume-dist) (g/over window))
                                      (-> (g/percent-rank) (g/over window)))
                                     g/collect-vals))))
    (is (every? int? (flatten (-> (df-20)
                                  (g/select
                                   (-> (g/rank) (g/over window))
                                   (-> (g/dense-rank) (g/over window))
                                   (-> (g/ntile 2) (g/over window)))
                                  g/collect-vals))))
    (let [actual (-> (df-20)
                     (g/select
                      (-> (g/lag :Price 1) (g/over window))
                      (-> (g/lag :Price 1 -999) (g/over window)))
                     g/collect-vals)]
      (is (and (nil? (ffirst actual))
               (= -999.0 (second (first actual))))))
    (let [actual (-> (df-20)
                     (g/select
                      (-> (g/lead :Price 1) (g/over window))
                      (-> (g/lead :Price 1 -999) (g/over window)))
                     g/collect-vals)]
      (is (and (nil? (first (last actual)))
               (= -999.0 (second (last actual)))))))
  (testing "shortcut windowed works"
    (is (= ["Biggin" "Collins" "Greg" "Jellis" "LITTLE" "Nelson"]
           (-> (df-20)
               (g/select
                :SellerG
                {:rank-by-suburb (g/windowed {:window-col   (g/rank)
                                              :partition-by :SellerG
                                              :order-by     (g/desc :Price)})})
               (g/filter (g/= :rank-by-suburb 1))
               (g/order-by :SellerG)
               (g/collect-col :SellerG))))))

(deftest windowing-test
  (testing "can instantiate empty WindowSpec"
    (is (instance? WindowSpec (g/window {}))))
  (let [records    (-> (df-20)
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
    (let [pairs (map vector price-gaps (rest price-gaps))]
      (is (every? #(< (first %) (second %)) pairs)))
    (is (= [1 2 3 4] (map :row-num records))))
  (testing "count rows last week"
    (is (= #{1 2 3}
           (-> (df-20)
               (g/select (-> (g/unix-timestamp :Date "d/MM/yyyy") (g/as :date)))
               (g/select
                (-> (g/count "*")
                    (g/over (g/window {:partition-by :date
                                       :order-by :date
                                       :range-between {:start (* -7 60 60 24) :end 0}}))))
               g/collect-vals
               flatten
               set))))
  (testing "count rows in the last two rows"
    (is (= #{1 2}
           (-> (df-20)
               (g/select (-> (g/unix-timestamp :Date "d/MM/yyyy") (g/as :date)))
               (g/select
                (-> (g/count "*")
                    (g/over (g/window {:partition-by :date
                                       :order-by :date
                                       :rows-between {:start 0 :end 1}}))))
               g/collect-vals
               flatten
               set)))))

(deftest time-functions-test
  (testing "correct time bucketisation"
    (let [dataframe (-> (df-20)
                        (g/with-column :date (g/to-date :Date "d/MM/yyyy")))]
      (is (<= 10 (-> dataframe
                     (g/select (g/time-window :date "7 days"))
                     g/distinct
                     g/count) 14))
      (is (<= 40 (-> dataframe
                     (g/select (g/time-window :date "7 days" "2 days"))
                     g/distinct
                     g/count) 44))
      (is (<= 28 (-> dataframe
                     (g/select (g/time-window :date "7 days" "3 days" "2 days"))
                     g/distinct
                     g/count) 32))))
  (testing "correct time arithmetic"
    (let [[x0 x1 x2 x3] (-> (df-1)
                            (g/select
                             (-> (g/to-timestamp (g/lit "2020-05-12")))
                             (-> (g/to-timestamp (g/lit "2020-05-12") "yyyy-MM-dd"))
                             (-> (g/to-date (g/lit "2020-05-12")))
                             (-> (g/to-date (g/lit "2020-05-12") "yyyy-MM-dd")))
                            g/collect-vals
                            first)]
      (is (and (instance? java.sql.Timestamp x0)
               (instance? java.sql.Timestamp x1)
               (instance? java.sql.Date x2)
               (instance? java.sql.Date x3))))
    (is (= ["2020-05-12 02:00"]
           (-> (df-1)
               (g/select {:utc (-> (g/to-timestamp (g/lit "2020-05-12 09:00:00"))
                                   (g/to-utc-timestamp "Asia/Jakarta")
                                   (g/date-format "yyyy-MM-dd HH:mm"))})
               (g/collect-col :utc))))
    (let [ts 1
          dt (-> (Instant/ofEpochMilli 1)
                 (.atZone (ZoneId/systemDefault)))]
      (is (.contains (-> (df-1)
                         (g/select (g/from-unixtime ts))
                         g/collect-vals
                         ffirst) (.format dt (DateTimeFormatter/ofPattern "yyyy-MM-dd "))))
      (is (= (.format dt (DateTimeFormatter/ofPattern "yyyy/MM/d HH:mm"))
             (-> (df-1)
                 (g/select (g/from-unixtime ts "yyyy/MM/d HH:mm"))
                 g/collect-vals
                 ffirst))))
    (is (= 2
           (-> (df-1)
               (g/select (g/quarter (g/lit "2020-05-12")))
               g/collect-vals
               ffirst)))
    (is (= (mod (-> (df-1)
                    (g/select (g/date-trunc "YYYY" (g/to-timestamp (g/lit "2020-05-12"))))
                    g/collect-vals
                    ffirst
                    .getTime) 10000) 0))
    (is (= [["2020-05-31"
             "2020-02-02"
             "2020-03-09"
             "2019~02~09"
             "2020-05-05"
             18
             23
             -3.0]]
           (-> (df-1)
               (g/select
                (-> (g/last-day (g/lit "2020-05-12")) (g/cast "string"))
                (-> (g/next-day (g/lit "2020-02-01") "Sunday") (g/cast "string"))
                (-> (g/lit "2020-03-02") (g/date-add 10) (g/date-sub 3) (g/cast "string"))
                (-> (g/date-format (g/lit "2019-02-09") "yyyy~MM~dd") (g/cast "string"))
                (-> (g/lit "2020-02-05") (g/add-months 3) (g/cast "string"))
                (g/week-of-year (g/lit "2020-04-30"))
                (g/round (g/date-diff (g/lit "2020-05-23") (g/lit "2020-04-30")))
                (g/round (g/months-between (g/lit "2020-01-23") (g/lit "2020-04-30"))))
               g/collect-vals))))
  (testing "correct current times"
    (let [actual (-> (df-1)
                     (g/select
                      (g/cast (g/current-timestamp) "string")
                      (g/cast (g/current-date) "string"))
                     g/collect-vals
                     flatten)]
      (is (and (clojure.string/includes? (first actual) ":")
               (not (clojure.string/includes? (second actual) ":"))))))
  (testing "correct time comparisons"
    (is (every? identity (flatten (-> (df-1)
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
                                      g/collect-vals)))))
  (testing "correct time extraction"
    (let [date (g/lit "1930-12-30 13:15:05")]
      (is (= {:day-of-month 30
              :day-of-week 3
              :day-of-year 364
              :hour 13
              :minute 15
              :month 12
              :second 5
              :year 1930}
             (-> (df-1)
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
                 first))))))

(deftest hashing-should-give-unique-rows-test
  (let [n-sellers (-> (df-20) (g/select :SellerG) g/distinct g/count)]
    (is (= n-sellers (-> (df-20) (g/select (g/xxhash64 :SellerG)) g/distinct g/count)))
    (is (= n-sellers (-> (df-20) (g/select (g/md5 :SellerG)) g/distinct g/count)))
    (is (= n-sellers (-> (df-20) (g/select (g/sha1 :SellerG)) g/distinct g/count)))
    (is (= n-sellers (-> (df-20) (g/select (g/sha2 :SellerG 256)) g/distinct g/count)))))
