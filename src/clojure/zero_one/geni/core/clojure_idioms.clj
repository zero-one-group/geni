(ns zero-one.geni.core.clojure-idioms
  (:refer-clojure :exclude [=
                            boolean
                            byte
                            case
                            cond
                            condp
                            dec
                            double
                            even?
                            float
                            if
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
                            vals
                            zero?
                            zipmap])
  (:require
   [potemkin :refer [import-fn]]
   [zero-one.geni.core.column :as column]
   [zero-one.geni.core.dataset :as dataset]
   [zero-one.geni.core.functions :as sql]
   [zero-one.geni.core.polymorphic :as polymorphic]
   [zero-one.geni.interop :as interop])
  (:import
   (org.apache.spark.sql functions)))

;; Collections
(defn remove
  "Returns a new Dataset that only contains elements where func returns false."
  [dataframe expr]
  (.filter dataframe (-> expr column/->column (.cast "boolean") functions/not)))

(defn rand-nth
  "Returns a random row collected."
  [dataframe]
  (let [small-frac (min 1.0 (/ 10.0 (.count dataframe)))]
    (-> dataframe (dataset/sample small-frac) (dataset/limit 1) dataset/head)))

;; Arithmetic
(defn inc
  "Returns an expression one greater than `expr`."
  [expr]
  (column/+ (column/->column expr) 1))

(defn dec
  "Returns an expression one less than `expr`."
  [expr]
  (column/- (column/->column expr) 1))

;; Casting
(defn short
  "Casts the column to a short."
  [expr]
  (column/cast (column/->column expr) "short"))

(defn int
  "Casts the column to an int."
  [expr]
  (column/cast (column/->column expr) "int"))

(defn long
  "Casts the column to a long."
  [expr]
  (column/cast (column/->column expr) "long"))

(defn float
  "Casts the column to a float."
  [expr]
  (column/cast (column/->column expr) "float"))

(defn double
  "Casts the column to a double."
  [expr]
  (column/cast (column/->column expr) "double"))

(defn boolean
  "Casts the column to a boolean."
  [expr]
  (column/cast (column/->column expr) "boolean"))

(defn byte
  "Casts the column to a byte."
  [expr]
  (column/cast (column/->column expr) "byte"))

(defn str
  "Casts the column to a str."
  [expr]
  (column/cast (column/->column expr) "string"))

;; Predicates
(defn zero?
  "Returns true if `expr` is zero, else false."
  [expr]
  (column/=== (column/->column expr) 0))

(defn pos?
  "Returns true if `expr` is greater than zero, else false."
  [expr]
  (column/< 0 (column/->column expr)))

(defn neg?
  "Returns true if `expr` is less than zero, else false."
  [expr]
  (column/< (column/->column expr) 0))

(defn even?
  "Returns true if `expr` is even, else false."
  [expr]
  (column/=== (column/mod (column/->column expr) 2) 0))

(defn odd?
  "Returns true if `expr` is odd, else false."
  [expr]
  (column/=== (column/mod (column/->column expr) 2) 1))

(import-fn column/=== =)

;; Map Operations
(defn merge
  "Variadic version of `map-concat`."
  [expr & ms]
  (reduce sql/map-concat expr ms))

(defn- rename-cols
  "Returns a new Dataset with columns renamed according to `kmap`."
  [k kmap]
  (concat
   (map
    (fn [[old-k new-k]]
      (sql/when (.equalTo (column/->column k) (column/->column old-k))
        (column/->column new-k)))
    kmap)
   [(column/->column k)]))

(defn rename-keys
  "Same as `transform-keys` with a map arg."
  [expr kmap]
  (sql/transform-keys
   expr
   (fn [k _] (functions/coalesce (column/->col-array (rename-cols k kmap))))))

(defn select-keys
  "Returns a map containing only those entries in map (`expr`) whose key is in `ks`."
  [expr ks]
  (sql/map-filter expr (fn [k _] (.isin k (interop/->scala-seq ks)))))

(import-fn sql/map-from-arrays zipmap)
(import-fn sql/map-keys keys)
(import-fn sql/map-values vals)
(import-fn sql/map-zip-with merge-with)

;; Common Macros
(defn cond
  "Returns a new Column imitating Clojure's `cond` macro behaviour."
  [& clauses]
  (let [predicates   (take-nth 2 clauses)
        then-cols    (take-nth 2 (rest clauses))
        whenned-cols (map (fn [pred then]
                            ;; clojure.core/if not available for some reason.
                            ;; this is a workaround using a map lookup with a default.
                            ({:else (column/->column then)} pred (sql/when pred then)))
                          predicates
                          then-cols)]
    (apply polymorphic/coalesce whenned-cols)))

(defn condp
  "Returns a new Column imitating Clojure's `condp` macro behaviour."
  [pred expr & clauses]
  (let [default      (when (clojure.core/odd? (count clauses))
                       (last clauses))
        test-exprs   (take-nth 2 clauses)
        then-cols    (take-nth 2 (rest clauses))
        whenned-cols (map #(sql/when
                            (pred (column/->column %1)
                                  (column/->column expr))
                             %2)
                          test-exprs
                          then-cols)]
    (apply polymorphic/coalesce (concat whenned-cols [(column/->column default)]))))

(defn case
  "Returns a new Column imitating Clojure's `case` macro behaviour."
  [expr & clauses]
  (let [default    (when (clojure.core/odd? (count clauses))
                     (last clauses))
        match-cols (take-nth 2 clauses)
        then-cols  (take-nth 2 (rest clauses))
        whenned-cols (map #(sql/when (column/=== %1 expr) %2) match-cols then-cols)]
    (apply polymorphic/coalesce (concat whenned-cols [(column/->column default)]))))

(import-fn sql/when if)

