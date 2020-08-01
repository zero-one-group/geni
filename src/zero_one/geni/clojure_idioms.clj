(ns zero-one.geni.clojure-idioms
  (:refer-clojure :exclude [=
                            boolean
                            byte
                            dec
                            double
                            even?
                            float
                            inc
                            int
                            keys
                            long
                            merge
                            merge-with
                            neg?
                            odd?
                            pos?
                            rand-nth
                            remove
                            rename-keys
                            select-keys
                            short
                            str
                            update
                            vals
                            zero?
                            zipmap])
  (:require
    [zero-one.geni.column :as column]
    [zero-one.geni.dataset :as dataset]
    [zero-one.geni.interop :as interop]
    [zero-one.geni.sql :as sql])
  (:import
    (org.apache.spark.sql functions)))

;; Collections
(defn remove [dataframe expr]
  (.filter dataframe (-> expr column/->column (.cast "boolean") functions/not)))

(defn rand-nth [dataframe]
  (let [small-frac (min 1.0 (/ 10.0 (.count dataframe)))]
    (-> dataframe (dataset/sample small-frac) (dataset/limit 1) dataset/head)))

;; Arithmetic
(defn inc [expr] (column/+ (column/->column expr) 1))
(defn dec [expr] (column/- (column/->column expr) 1))

;; Casting
(defn short [expr] (column/cast (column/->column expr) "short"))
(defn int [expr] (column/cast (column/->column expr) "int"))
(defn long [expr] (column/cast (column/->column expr) "long"))
(defn float [expr] (column/cast (column/->column expr) "float"))
(defn double [expr] (column/cast (column/->column expr) "double"))
(defn boolean [expr] (column/cast (column/->column expr) "boolean"))
(defn byte [expr] (column/cast (column/->column expr) "byte"))
(defn str [expr] (column/cast (column/->column expr) "string"))

;; Predicates
(defn = [l-expr r-expr] (column/=== (column/->column l-expr) (column/->column r-expr)))
(defn zero? [expr] (column/=== (column/->column expr) 0))
(defn pos? [expr] (column/< 0 (column/->column expr)))
(defn neg? [expr] (column/< (column/->column expr) 0))
(defn even? [expr] (column/=== (column/mod (column/->column expr) 2) 0))
(defn odd? [expr] (column/=== (column/mod (column/->column expr) 2) 1))

;; Map Operations
(def keys sql/map-keys)

(defn merge [expr & ms] (reduce sql/map-concat expr ms))

(def merge-with sql/map-zip-with)

(defn- rename-cols [k kmap]
  (concat
    (map
      (fn [[old-k new-k]]
        (sql/when (.equalTo (column/->column k) (column/->column old-k))
          (column/->column new-k)))
      kmap)
    [(column/->column k)]))

(defn rename-keys [expr kmap]
  (sql/transform-keys
    expr
    (fn [k _] (functions/coalesce (column/->col-array (rename-cols k kmap))))))

(defn select-keys [expr ks]
  (sql/map-filter expr (fn [k _] (.isin k (interop/->scala-seq ks)))))

(defn update [expr k f & args]
  (sql/transform-values
    expr
    (fn [k' v] (sql/when (.equalTo (column/->column k') (column/->column k))
                 (apply f v args)
                 v))))

(def vals sql/map-values)

(def zipmap sql/map-from-arrays)
