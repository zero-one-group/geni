(ns geni.cookbook-code
  (:require
    [clojure.java.io]
    [clojure.java.shell]
    [zero-one.geni.core :as g]))

(defn download-data! [source-url target-path]
  (if (-> target-path clojure.java.io/file .exists)
    :already-exists
    (do
      (clojure.java.io/make-parents target-path)
      (clojure.java.shell/sh "wget" "-O" target-path source-url)
      :downloaded)))

;; Part 1: Reading and Writing Datasets
(def bikes-data-url "https://raw.githubusercontent.com/jvns/pandas-cookbook/master/data/bikes.csv")
(def bikes-data-path "resources/cookbook/bikes.csv")
(download-data! bikes-data-url bikes-data-path)

;; 1.1 Creating a Spark Session
(defonce spark (g/create-spark-session {}))
(g/spark-conf spark)

;; 1.2 Reading Data from a CSV File

(def broken-df (g/read-csv! spark bikes-data-path))
(-> broken-df
    (g/limit 3)
    g/show)

(def fixed-df
  (g/read-csv! spark bikes-data-path {:delimiter ";" :encoding "ISO-8859-1"}))
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
      (g/to-df [:date
                :berri-1
                :brebeuf
                :cote-sainte-catherine
                :maisonneuve-1
                :maisonneuve-2
                :du-parc
                :pierre-dupuy
                :rachel-1
                :st-urbain])))

(-> renamed-df (g/limit 3) g/show)

;; 1.3 Writing Datasets
(g/write-parquet! renamed-df "resources/cookbook/bikes.parquet")

;; Part 2: Selecting Rows and Columns
(def complaints-data-url
  "https://raw.githubusercontent.com/jvns/pandas-cookbook/master/data/311-service-requests.csv")
(def complaints-data-path
  "resources/cookbook/complaints.csv")
(download-data! complaints-data-url complaints-data-path)

(def raw-complaints
  (g/read-csv! spark complaints-data-path))

;; 2.1 What's Even In It?
(g/show raw-complaints)

(count (g/columns raw-complaints))

(g/count raw-complaints)

(g/print-schema raw-complaints)

(require '[camel-snake-kebab.core])
(require '[clojure.string])

(defn normalise-column-names [dataset]
  (let [new-columns (->> raw-complaints
                         g/column-names
                         (map #(clojure.string/replace % #"\((.*?)\)" ""))
                         (map camel-snake-kebab.core/->kebab-case))]
    (g/to-df dataset new-columns)))

(def complaints
  (let [new-columns (->> raw-complaints
                         g/column-names
                         (map #(clojure.string/replace % #"\((.*?)\)" ""))
                         (map camel-snake-kebab.core/->kebab-case))]
    (g/to-df raw-complaints new-columns)))

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
