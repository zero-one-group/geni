(ns geni.cookbook-06
  (:require
   [clojure.java.io]
   [clojure.java.shell]
   [zero-one.geni.core :as g]
   [zero-one.geni.ml :as ml]))

(load-file "docs/cookbook/cookbook-util.clj")

(def complaints-data-url
  "https://raw.githubusercontent.com/jvns/pandas-cookbook/master/data/311-service-requests.csv")

(def complaints-data-path
  "data/cookbook/complaints.csv")

(download-data! complaints-data-url complaints-data-path)

;; Part 6: Cleaning Up Messy Data
(def complaints
  (g/read-csv! complaints-data-path {:kebab-columns true}))

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
