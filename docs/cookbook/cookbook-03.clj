(ns geni.cookbook-03
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

(def bikes-data-url "https://raw.githubusercontent.com/jvns/pandas-cookbook/master/data/bikes.csv")

(def bikes-data-path "data/cookbook/bikes.csv")

(download-data! bikes-data-url bikes-data-path)

;; 3.1 Adding a Weekday Column

(def bikes
  (g/read-csv! bikes-data-path {:delimiter ";"
                                :encoding "ISO-8859-1"
                                :kebab-columns true}))

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
