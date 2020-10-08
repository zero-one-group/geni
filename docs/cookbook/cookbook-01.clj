(ns geni.cookbook-01
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

(-> renamed-df
    g/describe
    g/show)

;; 1.4 Writing Datasets

(g/write-parquet! renamed-df "data/cookbook/bikes.parquet" {:mode "overwrite"})
