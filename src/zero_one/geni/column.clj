(ns zero-one.geni.column
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

;; TODO: explain->multimethod
;;;; Column Methods
(defn % [left-expr right-expr]
  (.mod (->column left-expr) (->column right-expr)))
(def mod %)

(defn && [& exprs]
  (reduce #(.and (->column %1) (->column %2))
          (lit true)
          (->col-array exprs)))

(defn * [& exprs]
  (reduce #(.multiply (->column %1) (->column %2))
          (lit 1)
          (->col-array exprs)))

(defn + [& exprs]
  (reduce #(.plus (->column %1) (->column %2))
          (lit 0)
          (->col-array exprs)))

(defn - [& exprs]
  (reduce #(.minus (->column %1) (->column %2))
          (->col-array exprs)))

(defn / [& exprs]
  (reduce #(.divide (->column %1) (->column %2))
          (->col-array exprs)))

(defn- compare-columns [compare-fn expr-0 & exprs]
  (let [exprs (-> exprs (conj expr-0))]
    (reduce
      (fn [acc-col [l-expr r-expr]]
        (&& acc-col (compare-fn (->column l-expr) (->column r-expr))))
      (lit true)
      (clojure.core/map vector exprs (rest exprs)))))

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

(defn & [left-expr right-expr]
  (.bitwiseAND (->column left-expr) (->column right-expr)))
(def bitwise-and &)

(defn | [left-expr right-expr]
  (.bitwiseOR (->column left-expr) (->column right-expr)))
(def bitwise-or |)

(defn bitwise-xor [left-expr right-expr]
  (.bitwiseXOR (->column left-expr) (->column right-expr)))

(defn cast [expr new-type] (.cast (->column expr) new-type))

(defn contains [expr literal] (.contains (->column expr) literal))

(defn ends-with [expr literal] (.endsWith (->column expr) literal))

(defn get-field [expr field-name] (.getField (->column expr) (name field-name)))

(defn get-item [expr k] (.getItem (->column expr) (try
                                                    (name k)
                                                    (catch Exception _ k))))

(defn is-in-collection [expr coll]
  (.isInCollection (->column expr) coll))

(defn is-nan [expr] (.isNaN (->column expr)))
(def nan? is-nan)

(defn is-not-null [expr] (.isNotNull (->column expr)))
(def not-null? is-not-null)

(defn is-null [expr] (.isNull (->column expr)))
(def null? is-null)

(defn isin [expr coll] (.isin (->column expr) (interop/->scala-seq coll)))

(defn like [expr literal] (.like (->column expr) literal))

(defn rlike [expr literal] (.rlike (->column expr) literal))

(defn starts-with [expr literal] (.startsWith (->column expr) literal))

(defn || [& exprs]
  (reduce #(.or (->column %1) (->column %2))
          (lit false)
          (->col-array exprs)))

;;;; Sorting Functions
(defn asc [expr] (.asc (->column expr)))
(defn asc-nulls-first [expr] (.asc_nulls_first (->column expr)))
(defn asc-nulls-last [expr] (.asc_nulls_last (->column expr)))
(defn desc [expr] (.desc (->column expr)))
(defn desc-nulls-first [expr] (.desc_nulls_first (->column expr)))
(defn desc-nulls-last [expr] (.desc_nulls_last (->column expr)))

;; Java Expressions
(defn between [expr lower-bound upper-bound]
  (.between (->column expr) lower-bound upper-bound))

;; Support Functions
(defn hash-code [expr] (.hashCode (->column expr)))

;; Shortcut Functions
(defn null-rate [expr]
  (-> expr
      ->column
      null?
      (cast "int")
      functions/mean
      (.as (clojure.core/str "null_rate(" (name expr) ")"))))

(defn null-count [expr]
  (-> expr
      ->column
      null?
      (cast "int")
      functions/sum
      (.as (clojure.core/str "null_count(" (name expr) ")"))))

;; Clojure Idioms
;;;; Arithmetic
(defn inc [expr] (+ (->column expr) 1))
(defn dec [expr] (- (->column expr) 1))

;;;; Casting
(defn short [expr] (cast (->column expr) "short"))
(defn int [expr] (cast (->column expr) "int"))
(defn long [expr] (cast (->column expr) "long"))
(defn float [expr] (cast (->column expr) "float"))
(defn double [expr] (cast (->column expr) "double"))
(defn boolean [expr] (cast (->column expr) "boolean"))
(defn byte [expr] (cast (->column expr) "byte"))
(defn str [expr] (cast (->column expr) "string"))

;;;; Predicates
(defn = [l-expr r-expr] (=== (->column l-expr) (->column r-expr)))
(defn zero? [expr] (=== (->column expr) 0))
(defn pos? [expr] (< 0 (->column expr)))
(defn neg? [expr] (< (->column expr) 0))
(defn even? [expr] (=== (mod (->column expr) 2) 0))
(defn odd? [expr] (=== (mod (->column expr) 2) 1))
