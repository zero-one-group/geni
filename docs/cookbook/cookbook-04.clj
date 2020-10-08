(ns geni.cookbook-04
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
