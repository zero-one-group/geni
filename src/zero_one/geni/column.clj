(ns zero-one.geni.column
  (:import
    (org.apache.spark.sql Column
                          Dataset
                          functions)))

(defmulti col (fn [head & _] (class head)))
(defmethod col :default [x & _] (functions/lit x))
(defmethod col Column [x & _] x)
(defmethod col java.lang.String [x & _] (functions/col x))
(defmethod col clojure.lang.Keyword [x & _] (functions/col (name x)))
(defmethod col Dataset [dataframe & args] (.col dataframe (first args)))

(defn ->col-array [columns]
  (->> columns (map col) (into-array Column)))
(def ->column col)
