(ns zero-one.geni.core.function-table
  "Spark's SQL functions from a table, a row per function, which
  `zero-one.geni.core.functions` holds. Each row names the Spark function, the
  Spark version that added it, when that's after 3.5, and its argument lists.
  A function calls Spark's `functions` method of that name by reflection, so
  `zero-one.geni.core` loads on any Spark and with only the Spark Connect
  client, and it takes whichever of the method's overloads on the classpath
  fits its arguments best."
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as string]
   [zero-one.geni.core.column :refer [->col-array ->column]]
   [zero-one.geni.core.dataset-creation :as dataset-creation]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.spark :as spark])
  (:import
   (java.lang.reflect InvocationTargetException Method Modifier)
   (org.apache.spark.sql Column functions)
   (org.apache.spark.sql.types DataType)))

;;;; Calling a function

(defn- usable-method?
  "Whether Geni can call the method: a public static one, without Scala's
  own types, such as a `Seq`, which `@varargs` methods also take as Java
  varargs, a function, or a type tag."
  [^Method method]
  (and (Modifier/isStatic (.getModifiers method))
       (Modifier/isPublic (.getModifiers method))
       (not-any? #(string/starts-with? (.getName ^Class %) "scala.")
                 (.getParameterTypes method))))

(def ^:private methods-named
  "The methods of Spark's `functions` that are called `spark-name`."
  (memoize
   (fn [spark-name]
     (->> (.getMethods functions)
          (filter #(= spark-name (.getName ^Method %)))
          (filter usable-method?)
          vec))))

(defn- int-sized
  "A whole number that fits an int as an int, so that it becomes an INT
  literal, as it does from Spark's Scala and Python APIs, which some
  functions need, such as `array_insert`'s position."
  [x]
  (if (and (instance? Long x) (<= Integer/MIN_VALUE x Integer/MAX_VALUE)) (int x) x))

(defn- option-string [x]
  (if (or (keyword? x) (symbol? x)) (name x) (str x)))

(defn- convert-arg
  "`value` for a parameter of class `t`, with a score for how well it fits, or
  nil when it doesn't. A keyword names a column, a string is a literal where
  Spark takes a string and a column name where it takes only a column, and a
  number goes as a primitive where Spark takes one, and as a literal column
  otherwise, an INT one when it fits."
  [^Class t value]
  (let [column-ish? (or (instance? Column value) (keyword? value) (symbol? value))]
    (cond
      (= t Column)
      (cond
        column-ish?     [(->column value) 4]
        (string? value) [(->column value) 1]
        (map? value)    nil
        :else           [(->column (int-sized value)) 2])

      (= t String)
      (cond
        (string? value)  [value 4]
        (keyword? value) [(name value) 1])

      (or (= t Integer/TYPE) (= t Integer))
      (when (and (integer? value) (<= Integer/MIN_VALUE value Integer/MAX_VALUE))
        [(int value) 4])

      (or (= t Long/TYPE) (= t Long))
      (when (integer? value) [(long value) 3])

      (or (= t Double/TYPE) (= t Double))
      (cond
        (float? value)  [(double value) 4]
        (number? value) [(double value) 2])

      (or (= t Float/TYPE) (= t Float))
      (when (number? value) [(float value) 2])

      (or (= t Boolean/TYPE) (= t Boolean))
      (when (boolean? value) [value 4])

      (= t Object)
      (cond
        column-ish?     [(->column value) 3]
        (map? value)    nil
        (coll? value)   [(interop/->java-array value) 3]
        :else           [(int-sized value) 3])

      (= t java.util.Map)
      (when (map? value)
        [(java.util.HashMap. ^java.util.Map (into {}
                                                  (map (fn [[k v]] [(option-string k) (option-string v)]))
                                                  value))
         4])

      (.isAssignableFrom DataType t)
      (cond
        (instance? t value)                  [value 4]
        (or (map? value) (vector? value))    (let [schema (dataset-creation/->schema value)]
                                               (when (instance? t schema) [schema 3])))

      (instance? t value)
      [value 2])))

(defn- convert-varargs
  "The rest of the arguments as the array that a Java varargs parameter of
  class `t` takes, with a score, or nil."
  [^Class t values]
  (let [component (.getComponentType t)]
    (cond
      (= component Column)
      [(->col-array (map int-sized values)) (* 3 (count values))]

      (= component String)
      (when (every? #(or (string? %) (keyword? %)) values)
        [(into-array String (map #(if (keyword? %) (name %) %) values)) (* 4 (count values))])

      :else
      (let [converted (map #(convert-arg component %) values)]
        (when (every? some? converted)
          [(into-array component (map first converted)) (reduce + (map second converted))])))))

(defn- fit
  "How the arguments fit `method`: their converted values and a score, or nil
  when they don't."
  [^Method method args]
  (let [params     (vec (.getParameterTypes method))
        varargs?   (.isVarArgs method)
        n-fixed    (if varargs? (dec (count params)) (count params))
        n-args     (count args)]
    (when (if varargs? (<= n-fixed n-args) (= n-fixed n-args))
      (let [fixed  (map convert-arg (take n-fixed params) (take n-fixed args))
            rest-v (when varargs? (convert-varargs (peek params) (drop n-fixed args)))]
        (when (and (every? some? fixed) (or (not varargs?) rest-v))
          {:method method
           :args   (cond-> (mapv first fixed) varargs? (conj (first rest-v)))
           ;; Spark's typed literals, rather than literal columns, on a tie.
           :score  [(+ (reduce + (map second fixed)) (or (second rest-v) 0))
                    (count (remove #(= Column %) params))]})))))

(defn- signature [^Method method]
  (str "(" (string/join ", " (map #(.getSimpleName ^Class %) (.getParameterTypes method))) ")"))

(defn invoke
  "Calls Spark's `functions` method `spark-name` with `args`, through the
  overload that fits them best, after checking that the Spark on the
  classpath is at least `since`, `[major minor]` or `[major minor patch]`,
  when that's given."
  [spark-name since geni-name args]
  (when since
    (spark/require-version! since (str geni-name)))
  (let [methods (methods-named spark-name)
        best    (->> methods
                     (keep #(fit % args))
                     (sort-by :score #(compare %2 %1))
                     first)]
    (if best
      (try
        (.invoke ^Method (:method best) nil (object-array (:args best)))
        (catch InvocationTargetException e
          (throw (or (.getCause e) e))))
      (throw (ex-info (str geni-name " takes the arguments of one of Spark's "
                           spark-name " methods on this Spark: "
                           (string/join ", " (sort (map signature methods)))
                           ". Got: " (pr-str args))
                      {:function geni-name :args args})))))

;;;; The table

(def docs
  "The functions' docstrings, keyed by Spark's name, from Spark's
  functions.scala, which `zero-one.geni.function-docs` in `dev/` writes."
  (delay
    (if-let [resource (io/resource "spark-function-docs.edn")]
      (with-open [reader (java.io.PushbackReader. (io/reader resource))]
        (edn/read reader))
      {})))

(defn table
  "The table's rows, as maps keyed by the Geni name, from the metadata of the
  functions that `def-spark-functions` defined, in the namespaces loaded, but
  not of their copies under other names, such as `->utc-timestamp`."
  []
  (into {}
        (for [n       (all-ns)
              [sym v] (ns-publics n)
              :let [{::keys [spark since row] :keys [arglists]} (meta v)]
              :when (and spark (= row sym))]
          [sym {:name sym :spark spark :since since :arglists (vec arglists)}])))

(defn- version-vector [since]
  (when since
    (mapv parse-long (string/split since #"\."))))

(defn- parse-row
  "A row: the Geni name, its argument lists, and then options, `:since` for
  the Spark version that added it, after 3.5, and `:spark` for Spark's name,
  when it isn't the Geni name in snake case."
  [[geni-name & more]]
  (let [arglists (vec (take-while vector? more))
        opts     (apply hash-map (drop-while vector? more))]
    {:name     geni-name
     :spark    (or (:spark opts) (string/replace (name geni-name) "-" "_"))
     :since    (:since opts)
     :arglists arglists}))

(defn- docstring [{:keys [spark since]}]
  (str (or (get @docs spark)
           (str "Spark's `" spark "` function."))
       "\n\nSpark's `functions." spark "`"
       (if since (str ", which needs Spark " since ".") ".")))

(defmacro def-spark-functions
  "Defines a function for each row of the table, with the row's Spark name
  and version in its metadata, for `table`. See `parse-row`. Each `defn` is a
  top-level form of its own, which AOT compilation keeps apart: more forms per
  row, or the table as one literal, would be more code than a JVM method can
  hold."
  [& rows]
  `(do
     ~@(for [{:keys [name spark since arglists] :as row} (map parse-row rows)]
         `(defn ~name
            ~(docstring row)
            {:arglists    '~(map (fn [arglist] (vec arglist)) arglists)
             ::spark      ~spark
             ::since      ~since
             ::row        '~name}
            [& args#]
            (invoke ~spark ~(version-vector since) '~name args#)))))
