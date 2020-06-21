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
(defmethod col Dataset [dataframe & args] (.col dataframe (name (first args))))
(def ->column col)

(defn ->col-seq [arg]
  (cond
    (map? arg)  (for [[k v] arg] (.as (->column v) (name k)))
    (coll? arg) (map ->column arg)
    :else       [(->column arg)]))

(defn ->col-array [args]
  (->> args
       (mapcat ->col-seq)
       (into-array Column)))
