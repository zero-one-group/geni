(ns geni.cookbook-code
  (:require
    [clojure.java.io]
    [clojure.java.shell]
    [zero-one.geni.core :as g]
    [zero-one.geni.ml :as ml]))

(defn download-data! [source-url target-path]
  (if (-> target-path clojure.java.io/file .exists)
    :already-exists
    (do
      (clojure.java.io/make-parents target-path)
      (clojure.java.shell/sh "wget" "-O" target-path source-url)
      :downloaded)))

;; Part 1: Reading and Writing Datasets
(def bikes-data-url "https://raw.githubusercontent.com/jvns/pandas-cookbook/master/data/bikes.csv")
(def bikes-data-path "data/cookbook/bikes.csv")
(download-data! bikes-data-url bikes-data-path)

;; 1.1 Reading Data from a CSV File
(def broken-df (g/read-csv! bikes-data-path))
(-> broken-df
    (g/limit 3)
    g/show)

(def fixed-df
  (g/read-csv! bikes-data-path {:delimiter ";" :encoding "ISO-8859-1"}))

(-> fixed-df
    (g/limit 3)
    g/show)

(-> fixed-df (g/limit 3) g/show-vertical)

(g/count fixed-df)

(g/print-schema fixed-df)

(-> fixed-df (g/limit 3) g/collect)

;; 1.2 Selecting and Renaming Columns
(-> fixed-df
    (g/select :Date "Berri 1")
    (g/limit 3)
    g/show)

(-> fixed-df
    (g/select {:date "Date" :berri-1 "Berri 1"})
    (g/limit 3)
    g/show)

(def renamed-df
  (-> fixed-df
      (g/to-df :date
               :berri-1
               :brebeuf
               :cote-sainte-catherine
               :maisonneuve-1
               :maisonneuve-2
               :du-parc
               :pierre-dupuy
               :rachel-1
               :st-urbain)))

(-> renamed-df (g/limit 3) g/show)

;; 1.3 Describing Columns

;; 1.4 Writing Datasets
(g/write-parquet! renamed-df "data/cookbook/bikes.parquet" {:mode "overwrite"})

;; Part 2: Selecting Rows and Columns
(def complaints-data-url
  "https://raw.githubusercontent.com/jvns/pandas-cookbook/master/data/311-service-requests.csv")
(def complaints-data-path
  "data/cookbook/complaints.csv")
(download-data! complaints-data-url complaints-data-path)

(def raw-complaints
  (g/read-csv! complaints-data-path))

;; 2.1 What's Even In It?
(g/show raw-complaints)

(count (g/columns raw-complaints))

(g/count raw-complaints)

(g/print-schema raw-complaints)

(def complaints
  (g/read-csv! complaints-data-path {:kebab-columns true}))

(g/print-schema complaints)

;;  2.2 Selecting Columns and Rows
(-> complaints
    (g/select :complaint-type)
    g/show)

(-> complaints
    (g/limit 5)
    g/show)

(-> complaints
    (g/select :complaint-type)
    (g/limit 5)
    g/show)

(-> complaints
    (g/limit 5)
    (g/select :complaint-type)
    g/show)

;;  2.3 Selecting Multiple Columns
(-> complaints
    (g/select :complaint-type :borough)
    g/show)

(-> complaints
    (g/select :complaint-type :borough)
    (g/limit 10)
    g/show)

;;  2.4 What's The Most Common Complaint Types?
(-> complaints
    (g/group-by :complaint-type)
    g/count
    g/show)

(-> complaints
    (g/group-by :complaint-type)
    g/count
    (g/order-by (g/desc :count))
    (g/limit 10)
    g/show)

(-> complaints
    (g/select :complaint-type)
    g/value-counts
    (g/limit 10)
    g/show)

;; 2.5 Selecting Only Noise Complaints
(-> complaints
    (g/filter (g/= :complaint-type (g/lit "Noise - Street/Sidewalk")))
    (g/select :complaint-type :borough :created-date :descriptor)
    (g/limit 3)
    g/show)

(-> complaints
    (g/filter (g/&&
                (g/= :complaint-type (g/lit "Noise - Street/Sidewalk"))
                (g/= :borough (g/lit "BROOKLYN"))))
    (g/select :complaint-type :borough :created-date :descriptor)
    (g/limit 3)
    g/show)

;; 2.6 Which Borough Has The Most Noise Complaints?
(-> complaints
    (g/filter (g/= :complaint-type (g/lit "Noise - Street/Sidewalk")))
    (g/select :borough)
    g/value-counts
    g/show)

;; Part 3: Grouping and Aggregating
(def bikes
  (g/read-csv! bikes-data-path {:delimiter ";"
                                :encoding "ISO-8859-1"
                                :kebab-columns true}))

;; 3.1 Adding a Weekday Column
(g/dtypes bikes)

(def berri-bikes
  (-> bikes
      (g/with-column :date (g/to-date :date "dd/M/yyyy"))
      (g/with-column :weekday (g/date-format :date "EEEE"))
      (g/select :date :weekday :berri-1)))

(g/dtypes berri-bikes)

(g/show berri-bikes)

;; 3.2 Adding Up The Cyclists By Weekday
(-> berri-bikes
    (g/group-by :weekday)
    (g/sum :berri-1)
    g/show)

(-> berri-bikes
    (g/group-by :weekday)
    (g/agg {:n-cyclists (g/sum :berri-1)})
    g/show)

;; Part 4: Reading and Writing Datasets

;; 4.1 Downloading One Month of Data
(defn weather-data-url [year month]
  (str "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?"
       "format=csv&stationID=5415&Year=" year "&Month=" month
       "&timeframe=1&submit=Download+Data"))

(defn weather-data-path [year month]
  (str "data/cookbook/weather/weather-" year "-" month ".csv"))

(defn weather-data [year month]
  (download-data! (weather-data-url year month) (weather-data-path year month))
  (g/read-csv! (weather-data-path year month) {:kebab-columns true}))

(def raw-weather-mar-2012 (weather-data 2012 3))
(g/print-schema raw-weather-mar-2012)
(g/count raw-weather-mar-2012)

;; 4.2 Dropping Columns with Nulls
(g/show raw-weather-mar-2012)

(def null-counts
  (-> raw-weather-mar-2012
      (g/agg (->> (g/column-names raw-weather-mar-2012)
                  (map #(vector % (g/null-count %)))
                  (into {})))
      g/first))

null-counts

(def columns-without-nulls
  (->> null-counts
       (filter #(zero? (second %)))
       (map first)))

(def columns-to-select
  (filter (set columns-without-nulls) (g/columns raw-weather-mar-2012)))

columns-to-select

(def weather-mar-2012
  (-> raw-weather-mar-2012
      (g/select columns-to-select)
      (g/drop :longitude
              :latitude
              :station-name
              :climate-id
              :time
              :data-quality)))
(g/show weather-mar-2012)

;; 4.3 Getting the Temperature by Hour of Day
(-> weather-mar-2012
    (g/with-column :hour (g/hour (g/to-timestamp :date-time "yyyy-M-d HH:mm")))
    (g/group-by :hour)
    (g/agg {:mean-temp (g/mean :temp-c)})
    (g/order-by :hour)
    (g/show {:num-rows 25}))

;; 4.4 Combining Monthly Data
(def weather-oct-2012
  (-> (weather-data 2012 10)
      (g/select (g/columns weather-mar-2012))))

(def weather-unioned
  (g/union weather-mar-2012 weather-oct-2012))

(g/count weather-unioned)

(-> weather-unioned
    (g/select :year :month)
    g/value-counts
    g/show)

;; 4.5 Joining by Day of Month
(defn average-by-day-of-month [dataset new-col-name]
  (-> dataset
      (g/group-by :day)
      (g/agg {new-col-name (g/mean :temp-c)})))

(def joined
  (g/join
    (average-by-day-of-month weather-mar-2012 :mean-temp-mar-2012)
    (average-by-day-of-month weather-oct-2012 :mean-temp-oct-2012)
    :day))

(-> joined
    (g/order-by :day)
    (g/show {:num-rows 25}))

;; 4.6 Reading Multiple Files at Once
(mapv (partial weather-data 2012) (range 1 13))

(def unioned
  (-> (g/read-csv! "data/cookbook/weather" {:kebab-columns true})
      (g/select (g/columns weather-mar-2012))))

(-> unioned
    (g/group-by :year :month)
    g/count
    (g/order-by :year :month)
    g/show)

(g/write-csv! unioned "data/cookbook/weather-2012.csv" {:mode "overwrite"})

;; Part 5: String Operations
(def weather-2012
  (g/read-csv! "data/cookbook/weather-2012.csv"))

;; 5.1 Finding The Snowiest Months
(-> weather-2012
    (g/filter (g/like (g/lower :weather) "%snow%"))
    (g/select :weather)
    g/distinct
    g/show)

(-> weather-2012
    (g/filter (g/like (g/lower :weather) "%snow%"))
    (g/group-by :year :month)
    (g/agg {:n-days (g/count-distinct :day)})
    (g/order-by :year :month)
    g/show)

;; 5.2 Putting Snowiness and Temperature Together
(-> weather-2012
    (g/group-by :year :month)
    (g/agg
      {:n-snow-days (g/count-distinct
                     (g/when (g/like (g/lower :weather) "%snow%") :day))
       :n-days      (g/count-distinct :day)
       :mean-temp   (g/mean :temp)})
    (g/order-by :year :month)
    (g/select {:year      :year
               :month     :month
               :snowiness (g/format-number (g// :n-snow-days :n-days) 2)
               :mean-temp (g/format-number :mean-temp 1)})
    g/show)

;; 5.3 Finding Temperatures of Common Weather Descriptions
(-> weather-2012
    (g/with-column :weather-description (g/explode (g/split :weather ",")))
    (g/group-by :weather-description)
    (g/agg {:mean-temp (g/mean :temp)
            :n-days    (g/count-distinct :year :month :day)})
    (g/order-by (g/desc :mean-temp))
    (g/select {:weather-description :weather-description
               :mean-temp (g/format-number :mean-temp 1)
               :n-days :n-days})
    (g/show {:num-rows 25}))

;; Part 6: Cleaning Up Messy Data
;(def complaints
  ;(g/read-csv! complaints-data-path {:kebab-columns true}))

;; 6.1 Messy Zip Codes
(-> complaints g/dtypes :incident-zip)

(-> complaints
    (g/select :incident-zip)
    g/distinct
    (g/collect-col :incident-zip)
    sort)

;; 6.2 Fixing NaN Values Confusion

(def faulty-zips ["NO CLUE" "N/A" "NA" "00000" "000000"])

(def fixed-faulty-zips
  (-> complaints
      (g/with-column
        :incident-zip
        (g/when (g/isin :incident-zip faulty-zips) nil :incident-zip))))

(-> fixed-faulty-zips
    (g/select :incident-zip)
    g/distinct
    (g/collect-col :incident-zip)
    sort)

;; 6.3 What's Up With The Dashes?
(-> fixed-faulty-zips
    (g/filter (g/=!= (g/length :incident-zip) 5))
    (g/select :incident-zip)
    g/distinct
    g/show)

(def fixed-dashed-zips
  (-> fixed-faulty-zips
      (g/with-column :incident-zip (g/substring :incident-zip 0 5))))

(-> fixed-dashed-zips
    (g/select :incident-zip)
    g/distinct
    (g/collect-col :incident-zip)
    sort)

(def close?
  (g/||
    (g/starts-with :incident-zip "0")
    (g/starts-with :incident-zip "1")))

(-> fixed-dashed-zips
    (g/filter (g/&& (g/not close?) (g/not-null? :incident-zip)))
    (g/select :incident-zip :descriptor :city)
    g/distinct
    (g/order-by :incident-zip)
    g/show)

(-> fixed-dashed-zips
    (g/select (g/upper :city))
    g/value-counts
    (g/show {:num-rows 40}))

;; 6.4 All In One Form
(-> complaints
    (g/with-column
      :incident-zip
      (g/when (g/isin :incident-zip faulty-zips)
        nil
        (g/substring :incident-zip 0 5)))
    (g/select :incident-zip)
    g/distinct
    (g/collect-col :incident-zip)
    sort)

;; Part 7: Timestamps and Dates
(def popularity-contest-data-url
  "https://raw.githubusercontent.com/jvns/pandas-cookbook/master/data/popularity-contest")

(def popularity-contest-data-path
  "data/cookbook/popularity-contest.csv")

(download-data! popularity-contest-data-url popularity-contest-data-path)

(def popularity-contest
  (-> (g/read-csv! popularity-contest-data-path {:delimiter " "})
      (g/to-df :access-time :creation-time :package-name :mru-program :tag)
      (g/remove (g/= :access-time (g/lit "END-POPULARITY-CONTEST-0")))))

(g/print-schema popularity-contest)

(-> popularity-contest (g/limit 5) g/show)

;; 7.1 Parsing Timestamps
(def formatted-popularity-contest
  (-> popularity-contest
      (g/with-column :access-time (g/to-timestamp (g/int :access-time)))
      (g/with-column :creation-time (g/to-timestamp (g/int :creation-time)))))

(g/print-schema formatted-popularity-contest)

(-> formatted-popularity-contest (g/limit 5) g/show)

(-> formatted-popularity-contest
    (g/select (g/year :access-time))
    g/value-counts
    g/show)

(def cleaned-popularity-contest
  (g/remove formatted-popularity-contest (g/< :access-time (g/to-timestamp 1))))

(-> cleaned-popularity-contest
    (g/select (g/year :access-time))
    g/value-counts
    g/show)

;; Part 8: Window Functions
(def product-revenue
  (g/table->dataset
    [["Thin"       "Cell phone" 6000]
     ["Normal"     "Tablet"     1500]
     ["Mini"       "Tablet"     5500]
     ["Ultra Thin" "Cell phone" 5000]
     ["Very Thin"  "Cell phone" 6000]
     ["Big"        "Tablet"     2500]
     ["Bendable"   "Cell phone" 3000]
     ["Foldable"   "Cell phone" 3000]
     ["Pro"        "Tablet"     4500]
     ["Pro2"       "Tablet"     6500]]
    [:product :category :revenue]))

(g/print-schema product-revenue)

;; 8.1 The Best and Second Best in Every Category

(def rank-by-category
  (g/windowed
    {:window-col   (g/dense-rank)
     :partition-by :category
     :order-by     (g/desc :revenue)}))

(-> product-revenue
    (g/with-column :rank-by-category rank-by-category)
    (g/filter (g/< :rank-by-category 3))
    g/show)

;; 8.2 Revenue Differences of Best and Second Best in Every Category
(def max-by-category
  (g/windowed
    {:window-col   (g/max :revenue)
     :partition-by :category}))

(-> product-revenue
    (g/with-column :max-by-category max-by-category)
    (g/with-column :revenue-diff (g/- :max-by-category :revenue))
    (g/order-by :category (g/desc :revenue))
    g/show)

;; 8.3 Revenue Differences to the Next Best in Every Category
(def next-best-by-category
  (g/windowed
    {:window-col   (g/lag :revenue 1)
     :partition-by :category
     :order-by     (g/desc :revenue)}))

(-> product-revenue
    (g/with-column :next-best-by-category next-best-by-category)
    (g/with-column :revenue-diff (g/- :next-best-by-category :revenue))
    g/show)

;; 8.4 Underperformance by One Sigma in Every Category
(def mean-by-category
  (g/windowed {:window-col (g/mean :revenue) :partition-by :category}))

(def std-by-category
  (g/windowed {:window-col (g/stddev :revenue) :partition-by :category}))

(-> product-revenue
    (g/with-column
      :z-stat-by-category
      (g// (g/- :revenue mean-by-category) std-by-category))
    (g/filter (g/< :z-stat-by-category -1))
    g/show)

;; Part 9: Loading Data From SQL Databases
(download-data!
  "https://cdn.sqlitetutorial.net/wp-content/uploads/2018/03/chinook.zip"
  "data/chinook.zip")

(when-not (-> "data/chinook.db" clojure.java.io/file .exists)
  (clojure.java.shell/sh "unzip" "data/chinook.zip" "-d" "data/"))

;; 9.1 Reading From SQLite
(def chinook-tracks
  (g/read-jdbc! {:driver        "org.sqlite.JDBC"
                 :url           "jdbc:sqlite:data/chinook.db"
                 :dbtable       "tracks"
                 :kebab-columns true}))

(g/count chinook-tracks)

(g/print-schema chinook-tracks)

(g/show chinook-tracks {:num-rows 3})

;; 9.2 Writing to SQLite
(g/write-jdbc! chinook-tracks
               {:driver  "org.sqlite.JDBC"
                :url     "jdbc:sqlite:data/chinook-tracks.sqlite"
                :dbtable "tracks"})

;; Part 10: Avoiding Repeated Computations with Caching

(def dummy-data-path "/data/performance-benchmark-data")

(def transactions (g/read-parquet! dummy-data-path))

(g/count transactions)

(g/print-schema transactions)

;; 10.1 Putting Together A Member Profile

(def member-spending
  (-> transactions
      (g/with-column :sales (g/* :price :quantity))
      (g/group-by :member-id)
      (g/agg {:total-spend     (g/sum :sales)
              :avg-basket-size (g/mean :sales)
              :avg-price       (g/mean :price)})))

(def member-frequency
  (-> transactions
      (g/group-by :member-id)
      (g/agg {:n-transactions (g/count "*")
              :n-visits       (g/count-distinct :date)})))

(def member-profile
  (g/join member-spending member-frequency :member-id))

(g/print-schema member-profile)

;; 10.2 Caching Intermediate Results

(defn some-other-computations [member-profile]
  (g/write-parquet! member-profile "data/temp.parquet" {:mode "overwrite"}))

(doall (for [_ (range 5)]
         (time (some-other-computations member-profile))))
; "Elapsed time: 10083.047244 msecs"
; "Elapsed time: 8231.45662 msecs"
; "Elapsed time: 8525.947692 msecs"
; "Elapsed time: 8155.982435 msecs"
; "Elapsed time: 7638.144858 msecs"

(def cached-member-profile
  (g/cache member-profile))

(doall (for [_ (range 5)]
         (time (some-other-computations cached-member-profile))))
; "Elapsed time: 11996.307581 msecs"
; "Elapsed time: 988.958567 msecs"
; "Elapsed time: 1017.365143 msecs"
; "Elapsed time: 1032.578846 msecs"
; "Elapsed time: 1087.077004 msecs"

;; Part 11: Basic ML Pipelines
(download-data!
  "https://raw.githubusercontent.com/ageron/handson-ml/master/datasets/housing/housing.csv"
  "data/houses.csv")

(def houses
  (-> (g/read-csv! "data/houses.csv" {:kebab-columns true})
      (g/with-column :rooms-per-house (g// :total-rooms :households))
      (g/with-column :population-per-house (g// :population :households))
      (g/with-column :bedrooms-per-house (g// :total-bedrooms :households))
      (g/drop :total-rooms :households :population :total-bedrooms)
      (g/with-column :median-income (g/double :median-income))
      (g/with-column :median-house-value (g/double :median-house-value))
      (g/with-column :housing-median-age (g/double :housing-median-age))))

(g/print-schema houses)
; root
;  |-- longitude: double (nullable = true)
;  |-- latitude: double (nullable = true)
;  |-- housing-median-age: double (nullable = true)
;  |-- median-income: double (nullable = true)
;  |-- median-house-value: double (nullable = true)
;  |-- ocean-proximity: string (nullable = true)
;  |-- rooms-per-house: double (nullable = true)
;  |-- population-per-house: double (nullable = true)
;  |-- bedrooms-per-house: double (nullable = true)

;; 11.1 Splitting into Train and Validation Sets

(def houses-splits (g/random-split houses [0.8 0.2] 1234))
(def training-data (first houses-splits))
(def test-data (second houses-splits))

(g/count training-data)
; 16525

(g/count test-data)
; 4115

;; 11.2 Model Pipelines
(def assembler
  (ml/vector-assembler {:input-cols [:housing-median-age
                                     :median-income
                                     :bedrooms-per-house
                                     :population-per-house]
                        :output-col :raw-features
                        :handle-invalid "skip"}))

(def scaler
  (ml/standard-scaler {:input-col :raw-features
                       :output-col :features
                       :with-mean true
                       :with-std true}))

(def random-forest
  (ml/random-forest-regressor {:label-col :median-house-value
                               :features-col :features}))

(def pipeline
  (ml/pipeline assembler scaler random-forest))

(def pipeline-model (ml/fit training-data pipeline))

(def predictions
  (-> test-data
      (ml/transform pipeline-model)
      (g/select :prediction :median-house-value)
      (g/with-column :error (g/- :prediction :median-house-value))))

(-> predictions (g/limit 5) g/show)
; +------------------+------------------+-----------------+
; |prediction        |median-house-value|error            |
; +------------------+------------------+-----------------+
; |124351.25434440118|85800.0           |38551.25434440118|
; |166946.9353283479 |111400.0          |55546.9353283479 |
; |135896.6548560019 |70500.0           |65396.6548560019 |
; |195527.8273169201 |128900.0          |66627.8273169201 |
; |214557.50504524485|116100.0          |98457.50504524485|
; +------------------+------------------+-----------------+

(let [evaluator (ml/regression-evaluator {:label-col :median-house-value
                                          :metric-name "mae"})]
  (println (format "MAE: %.2f" (ml/evaluate predictions evaluator))))
; MAE: 54554.34


;; 11.3 Random Forest Feature Importances
(def feature-importances
  (->> pipeline-model
       ml/stages
       last
       ml/feature-importances
       (zipmap (ml/input-cols assembler))))

feature-importances
; {"housing-median-age" 0.060262475752573055,
;  "median-income" 0.7847621702619059,
;  "bedrooms-per-house" 0.010547166447551434,
;  "population-per-house" 0.14442818753796965
