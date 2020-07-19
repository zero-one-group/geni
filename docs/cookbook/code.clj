(ns geni.cookbook
  (:require
    [clojure.java.io]
    [clojure.java.shell]
    [zero-one.geni.core :as g]))

;; Part 1: Reading and Writing Datasets
(def bikes-data-path "resources/cookbook/bikes.csv")

(defn download-bikes-data! []
  (let [data-url "https://raw.githubusercontent.com/jvns/pandas-cookbook/master/data/bikes.csv"]
    (if (-> bikes-data-path clojure.java.io/file .exists)
      :already-exists
      (do
        (clojure.java.io/make-parents bikes-data-path)
        (clojure.java.shell/sh "wget" "-O" bikes-data-path data-url)
        :downloaded))))
(download-bikes-data!)

;; 1.1 Creating a Spark Session
(defonce spark (g/create-spark-session {}))

(g/spark-conf spark)

;; 1.2 Reading data from a csv file
(def broken-df (g/read-csv! spark bikes-data-path))
(-> broken-df
    (g/limit 3)
    g/show)

(def fixed-df
  (g/read-csv! spark bikes-data-path {:delimiter ";" :encoding "ISO-8859-1"}))
(-> fixed-df
    (g/limit 3)
    g/show)

(-> fixed-df
    (g/limit 3)
    g/show-vertical)
(g/count fixed-df)
(g/print-schema fixed-df)

;; 1.2 Selecting and renaming columns
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

(-> renamed-df
    (g/limit 1)
    g/show-vertical)

;; 1.3 Writing Datasets
(-> renamed-df
    (g/coalesce 1)
    (g/write-parquet! "resources/cookbook/bikes.parquet"))
