(ns zero-one.geni.core.column
  (:refer-clojure :exclude [*
                            +
                            -
                            /
                            <
                            <=
                            =
                            >
                            >=
                            boolean
                            byte
                            cast
                            dec
                            double
                            even?
                            float
                            inc
                            int
                            long
                            mod
                            neg?
                            odd?
                            pos?
                            short
                            str
                            zero?])
  (:require
    [potemkin :refer [import-fn]]
    [zero-one.geni.docs :as docs]
    [zero-one.geni.interop :as interop])
  (:import
    (org.apache.spark.sql Column
                          Dataset
                          functions)))

;;;; Coercions
(defn lit [arg]
  (let [normed-arg (if (coll? arg) (into-array (type (first arg)) arg) arg)]
    (functions/lit normed-arg)))

(defmulti col (fn [head & _] (class head)))
(defmethod col :default [x & _] (lit x))
(defmethod col Column [x & _] x)
(defmethod col java.lang.String [x & _] (functions/col x))
(defmethod col clojure.lang.Keyword [x & _] (functions/col (name x)))
(defmethod col Dataset [dataframe & args] (.col dataframe (name (first args))))

(defn ->col-seq
  "Coerce a value into a coll of columns."
  [arg]
  (cond
    (map? arg)  (for [[k v] arg] (.as (col v) (name k)))
    (coll? arg) (map col arg)
    :else       [(col arg)]))

(defn ->col-array
  "Coerce a coll of coerceable values into a coll of columns."
  [args]
  (->> args
       (mapcat ->col-seq)
       (into-array Column)))

;;;; Column Methods
(defn % [left-expr right-expr]
  (.mod (col left-expr) (col right-expr)))
(def mod %)

(defn && [& exprs]
  (reduce #(.and (col %1) (col %2))
          (lit true)
          (->col-array exprs)))

(defn * [& exprs]
  (reduce #(.multiply (col %1) (col %2))
          (lit 1)
          (->col-array exprs)))

(defn + [& exprs]
  (reduce #(.plus (col %1) (col %2))
          (lit 0)
          (->col-array exprs)))

(defn minus [& exprs]
  (reduce #(.minus (col %1) (col %2))
          (->col-array exprs)))

(defn / [& exprs]
  (reduce #(.divide (col %1) (col %2))
          (->col-array exprs)))

(defn- compare-columns [compare-fn expr-0 & exprs]
  (let [exprs (-> exprs (conj expr-0))]
    (reduce
      (fn [acc-col [l-expr r-expr]]
        (&& acc-col (compare-fn (col l-expr) (col r-expr))))
      (lit true)
      (map vector exprs (rest exprs)))))

(def === (partial compare-columns #(.equalTo %1 %2)))
(def equal-to ===)

(def <=> (partial compare-columns #(.eqNullSafe %1 %2)))
(def eq-null-safe <=>)

(def =!= (partial compare-columns #(.notEqual %1 %2)))
(def not-equal <=>)

(def < (partial compare-columns #(.lt %1 %2)))
(def lt <)

(def <= (partial compare-columns #(.leq %1 %2)))
(def leq <=)

(def > (partial compare-columns #(.gt %1 %2)))
(def gt >)

(def >= (partial compare-columns #(.geq %1 %2)))
(def geq >=)

(defn bitwise-and [left-expr right-expr]
  (.bitwiseAND (col left-expr) (col right-expr)))

(defn bitwise-or [left-expr right-expr]
  (.bitwiseOR (col left-expr) (col right-expr)))

(defn bitwise-xor [left-expr right-expr]
  (.bitwiseXOR (col left-expr) (col right-expr)))

(defn cast [expr new-type] (.cast (col expr) new-type))

(defn contains [expr literal] (.contains (col expr) literal))

(defn ends-with [expr literal] (.endsWith (col expr) literal))

(defn get-field [expr field-name] (.getField (col expr) (name field-name)))

(defn get-item [expr k] (.getItem (col expr) (try
                                               (name k)
                                               (catch Exception _ k))))

(defn is-in-collection [expr coll]
  (.isInCollection (col expr) coll))

(defn is-na-n [expr] (.isNaN (col expr)))

(defn is-not-null [expr] (.isNotNull (col expr)))

(defn is-null [expr] (.isNull (col expr)))

(defn isin [expr coll] (.isin (col expr) (interop/->scala-seq coll)))

(defn like [expr literal] (.like (col expr) literal))

(defn rlike [expr literal] (.rlike (col expr) literal))

(defn starts-with [expr literal] (.startsWith (col expr) literal))

(defn || [& exprs]
  (reduce #(.or (col %1) (col %2))
          (lit false)
          (->col-array exprs)))

;;;; Sorting Functions
(defn asc [expr] (.asc (col expr)))
(defn asc-nulls-first [expr] (.asc_nulls_first (col expr)))
(defn asc-nulls-last [expr] (.asc_nulls_last (col expr)))
(defn desc [expr] (.desc (col expr)))
(defn desc-nulls-first [expr] (.desc_nulls_first (col expr)))
(defn desc-nulls-last [expr] (.desc_nulls_last (col expr)))

;; Java Expressions
(defn between [expr lower-bound upper-bound]
  (.between (col expr) lower-bound upper-bound))

;; Support Functions
(defn hash-code [expr] (.hashCode (col expr)))

;; Shortcut Functions
(defn null-rate
  "Aggregate function: returns the null rate of a column."
  [expr]
  (-> expr
      col
      is-null
      (cast "int")
      functions/mean
      (.as (clojure.core/str "null_rate(" (name expr) ")"))))

(defn null-count
  "Aggregate function: returns the null count of a column."
  [expr]
  (-> expr
      col
      is-null
      (cast "int")
      functions/sum
      (.as (clojure.core/str "null_count(" (name expr) ")"))))

;; Docs
(docs/alter-docs-in-ns!
  'zero-one.geni.core.column
  [(-> docs/spark-docs :methods :core :column)])

(docs/add-doc!
  (var col)
  (-> docs/spark-docs :methods :core :functions :col))

(docs/add-doc!
  (var lit)
  (-> docs/spark-docs :methods :core :functions :lit))

;; Aliases
(import-fn bitwise-and &)
(import-fn bitwise-or |)
(import-fn is-na-n is-nan)
(import-fn is-na-n nan?)
(import-fn is-not-null not-null?)
(import-fn is-null null?)
(import-fn minus -)
(import-fn col ->column)
