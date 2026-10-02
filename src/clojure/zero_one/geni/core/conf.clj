(ns zero-one.geni.core.conf
  "The session's runtime configs: Spark's `spark.conf()`, which Spark SQL
  reads as queries run, over Spark Connect too. `g/spark-conf` returns all
  the configs that are set."
  (:require
   [zero-one.geni.defaults :as defaults])
  (:import
   (org.apache.spark.sql RuntimeConfig SparkSession)
   (scala Option)))

(defn- session-and-args
  "Splits off a leading SparkSession, or takes Geni's default session."
  [args]
  (if (instance? SparkSession (first args))
    [(first args) (rest args)]
    [@defaults/spark args]))

(defn- runtime-conf ^RuntimeConfig [^SparkSession spark]
  (.conf spark))

(defn- conf-value [value]
  (cond
    (keyword? value)                         (name value)
    (or (string? value) (boolean? value))    value
    (instance? Long value)                   value
    (instance? Integer value)                (long value)
    :else                                    (str value)))

(defn conf-get
  "Returns the value of the config `k` as a string. Without a `default`, it's
  the value that's set, or else Spark's own default for the config, or nil
  when Spark doesn't know it. With a `default`, it's the value that's set, or
  else `default`, whatever Spark's own default is.

  ```clojure
  (g/conf-get \"spark.sql.shuffle.partitions\")
  => \"200\"
  (g/conf-get spark :spark.sql.ansi.enabled)
  ```"
  {:arglists '([k] [k default] [spark k] [spark k default])}
  [& args]
  (let [[spark [k & more]] (session-and-args args)
        conf               (runtime-conf spark)]
    (if (seq more)
      (let [default (first more)]
        (.get conf (name k) (some-> default conf-value str)))
      (let [^Option value (.getOption conf (name k))]
        (when (.isDefined value) (.get value))))))

(defn conf-set!
  "Sets the config `k` to `value`, or each config in a map of keys to values,
  for the session. A value can be a string, a number, a boolean or a keyword.
  Spark refuses a static config, such as `spark.sql.warehouse.dir`, and a value
  of the wrong type for a config it knows.

  ```clojure
  (g/conf-set! \"spark.sql.shuffle.partitions\" 8)
  (g/conf-set! spark {:spark.sql.ansi.enabled false})
  ```"
  {:arglists '([k value] [configs] [spark k value] [spark configs])}
  [& args]
  (let [[spark more] (session-and-args args)
        conf         (runtime-conf spark)
        configs      (if (map? (first more)) (first more) {(first more) (second more)})]
    (doseq [[k value] configs]
      (.set conf (name k) (conf-value value)))))

(defn conf-unset!
  "Unsets the config `k`, so that it goes back to Spark's own default."
  {:arglists '([k] [spark k])}
  [& args]
  (let [[spark [k]] (session-and-args args)]
    (.unset (runtime-conf spark) (name k))))

(defn conf-modifiable?
  "Returns true when the session can set the config `k`: Spark knows it, and
  it isn't static. A config Spark doesn't know returns false, though
  `conf-set!` still sets it."
  {:arglists '([k] [spark k])}
  [& args]
  (let [[spark [k]] (session-and-args args)]
    (.isModifiable (runtime-conf spark) (name k))))
