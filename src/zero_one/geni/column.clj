(ns zero-one.geni.column
  (:import
    (org.apache.spark.sql Column functions)))

(defmulti col class)
(defmethod col :default [x] (functions/lit x))
(defmethod col org.apache.spark.sql.Column [x] x)
(defmethod col java.lang.String [x] (functions/col x))
(defn ->col-array [columns]
  (->> columns (clojure.core/map col) (into-array Column)))
(def ->column col)
