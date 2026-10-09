(ns zero-one.geni.interop
  (:require
   [clojure.set :as set]
   [clojure.string :as string :refer [replace-first]]
   [clojure.walk :as walk]
   [zero-one.geni.docs :as docs]
   [zero-one.geni.utils :refer [->kebab-case class-named ensure-coll]])
  (:import
   (clojure.lang Reflector)
   (com.fasterxml.jackson.databind ObjectMapper)
   (java.io ByteArrayOutputStream PrintStream)
   (org.apache.spark.sql Row)
   (org.apache.spark.sql.types ArrayType MapType StructField StructType UserDefinedType)
   (scala Console
          Function0
          Function1
          Function2
          Function3
          Tuple2
          Tuple3)
   (scala.collection JavaConverters Map Seq)
   (scala.collection.immutable List ListMap ListMap$)))

(declare ->clojure)

(defn ->java-list [coll]
  (java.util.ArrayList. coll))

(defn scala-seq->vec [scala-seq]
  (vec (JavaConverters/seqAsJavaList scala-seq)))

(defn scala-map->map [^Map m]
  (into {}
        (for [[k v] (JavaConverters/mapAsJavaMap m)]
          [k (->clojure v)])))

(defn ->scala-seq
  "An immutable Scala List, which Spark's methods take as a Seq on both Scala
  2.12 and 2.13."
  ^List [coll]
  (.toList (JavaConverters/asScalaBuffer (vec coll))))

(defn- java-array-error [message value]
  (throw (ex-info (str message " Got: " (pr-str value)) {:value value})))

(defn- array-coll?
  "Whether `x` becomes a nested array: a collection other than a map."
  [x]
  (and (coll? x) (not (map? x))))

(defn- array-leaves
  "The values in a collection that aren't collections, however deeply."
  [coll]
  (mapcat #(if (array-coll? %) (array-leaves %) [%]) coll))

(def ^:private same-class-numbers #{Long Integer Short Byte Double Float BigDecimal})

(defn- mixed-types! [coll]
  (java-array-error (str "A collection that becomes an array literal needs values of one type, "
                         "or all numbers.")
                    coll))

(defn- leaf-type
  "The element class of an array of these values, at any depth, and the
  function that converts each to it, as Clojure's arithmetic would widen
  them: to doubles with a float or a double among them, to BigDecimals with a
  BigDecimal or a ratio, to longs for whole numbers that fit, and otherwise
  to BigDecimals. Other values need one class."
  [coll leaves]
  (let [classes (set (map class leaves))]
    (cond
      (empty? classes)
      (java-array-error (str "Spark can't tell the element type of a collection that's empty "
                             "or all nils, so pass a typed Java array instead, such as "
                             "(long-array 0), or (into-array [(long-array 0)]) for an array "
                             "of arrays.")
                        coll)

      (not-every? number? leaves)
      (if (= 1 (count classes))
        [(first classes) identity]
        (mixed-types! coll))

      ;; Numbers of one class that Spark's lit takes keep it.
      (and (= 1 (count classes)) (same-class-numbers (first classes)))
      [(first classes) identity]

      (some #(or (instance? Double %) (instance? Float %)) leaves)
      [Double double]

      (some #(or (decimal? %) (ratio? %)) leaves)
      [BigDecimal #(try
                     (bigdec %)
                     (catch ArithmeticException _
                       (java-array-error (str "A collection that becomes an array literal of "
                                              "decimals can't hold " (pr-str %) ", which has no "
                                              "exact decimal.")
                                         coll)))]

      (every? #(or (instance? Long %) (instance? Integer %) (instance? Short %)
                   (instance? Byte %) (<= Long/MIN_VALUE % Long/MAX_VALUE))
              leaves)
      [Long long]

      :else
      [BigDecimal bigdec])))

(defn- check-decimal!
  "Throws when Spark's DECIMAL(38,18), which it gives an array literal of
  decimals, can't hold `d` exactly: it would round it, or make it a null."
  [coll ^BigDecimal d]
  (let [d (.stripTrailingZeros d)]
    (when (or (< 18 (.scale d)) (< 20 (- (.precision d) (max 0 (.scale d)))))
      (java-array-error (str "Spark gives an array literal of decimals the type DECIMAL(38, 18), "
                             "which can't hold " (.toPlainString d) " exactly. Use doubles, or "
                             "build the array with g/array of g/lit values, which keeps their "
                             "digits, as g/sql's args take it from Spark 4.0.")
                        coll))
    d))

(defn ->java-array
  "A Java array of the collection's values, which Spark's `lit` takes as an
  array literal: keywords become their names, nils stay, a nested collection
  becomes a nested array, and numbers widen as Clojure's arithmetic widens
  them, at any depth: to doubles with a double among them, to BigDecimals
  with a BigDecimal or a ratio, and otherwise to longs. A decimal that
  Spark's DECIMAL(38,18) for such an array can't hold exactly throws, and so
  does a collection that's empty or all nils, one that holds maps, and one
  whose values have different types, since Spark can't type its array."
  [coll]
  (when (some map? (tree-seq array-coll? seq coll))
    (java-array-error (str "A collection that becomes an array literal can't hold maps: build "
                           "the array with g/array and g/map.")
                      coll))
  (let [leaves             (map #(if (keyword? %) (name %) %) (remove nil? (array-leaves coll)))
        [leaf-class convert] (leaf-type coll leaves)
        convert            (if (= BigDecimal leaf-class)
                             (comp #(check-decimal! coll %) convert)
                             convert)
        build              (fn build [c]
                             (if (some array-coll? c)
                               (let [arrays  (map #(cond
                                                     (nil? %)        nil
                                                     (array-coll? %) (build %)
                                                     :else           (mixed-types! coll))
                                                  c)
                                     classes (set (map class (remove nil? arrays)))]
                                 (when (< 1 (count classes))
                                   (mixed-types! coll))
                                 (into-array ^Class (first classes) arrays))
                               (into-array leaf-class
                                           (map #(when (some? %)
                                                   (convert (if (keyword? %) (name %) %)))
                                                c))))]
    (build coll)))

(defn ->scala-list-map
  "An immutable Scala ListMap of the key-value pairs, which keeps their order,
  on Scala 2.12 and 2.13."
  ^ListMap [pairs]
  (reduce (fn [^ListMap m [k v]] (.updated m k v))
          (.empty ListMap$/MODULE$)
          pairs))

(defn scala-object
  "The Scala object that a class such as `\"scala.None$\"` holds, its
  MODULE$, or nil when the class isn't on the classpath."
  [class-name]
  (some-> (class-named class-name) (.getField "MODULE$") (.get nil)))

(defn ->scala-tuple2 [coll]
  (Tuple2. (first coll) (second coll)))

(defn scala-tuple->vec [p]
  (->> (.productArity p)
       (range)
       (map #(.productElement p %))
       (into [])))

(defn ->scala-function0 [f]
  (reify Function0 (apply [_] (f))))

(defn ->scala-function1 [f]
  (reify Function1 (apply [_ x] (f x))))

(defn ->scala-function2 [f]
  (reify Function2 (apply [_ x y] (f x y))))

(defn ->scala-function3 [f]
  (reify Function3 (apply [_ x y z] (f x y z))))

(defn optional->nillable [value]
  (when (.isPresent value)
    (.get value)))

(defmacro with-scala-out-str [& body]
  `(let [out-buffer# (ByteArrayOutputStream.)
         ;; In UTF-8, as it's read back, whatever the platform's charset.
         out#        (PrintStream. out-buffer# true "UTF-8")]
     (Console/withOut
      out#
      (->scala-function0 (fn [] ~@body)))
     (.flush out#)
     (.toString out-buffer# "UTF-8")))

;; Spark brings Jackson, so JSON needs no extra dependency. Whole numbers come
;; back as Integer, Long or BigInteger, whichever fits, so that a DECIMAL(38,0)
;; survives.
(def ^:private ^ObjectMapper object-mapper (ObjectMapper.))

(defn- jackson->clojure [x]
  (cond
    (instance? java.util.Map x)  (into {} (map (fn [[k v]] [(keyword k) (jackson->clojure v)])) x)
    (instance? java.util.List x) (mapv jackson->clojure x)
    :else                        x))

(defn read-json
  "Parses a JSON string into Clojure data, with keywords for the keys."
  [^String json-str]
  (jackson->clojure (.readValue object-mapper json-str Object)))

(defn- json-key [k]
  (if (keyword? k) (subs (str k) 1) (str k)))

(defn- clojure->jackson [x]
  (cond
    (map? x)     (let [m (java.util.LinkedHashMap.)]
                   (doseq [[k v] x]
                     (.put m (json-key k) (clojure->jackson v)))
                   m)
    (keyword? x) (json-key x)
    (coll? x)    (java.util.ArrayList. ^java.util.Collection (mapv clojure->jackson x))
    :else        x))

(defn write-json
  "Writes Clojure data as a JSON string, with keywords as strings."
  ^String [x]
  (.writeValueAsString object-mapper (clojure->jackson x)))

(defn nested-types
  "Spark type `dt` and every type inside it: an array's elements', a map's
  keys' and values', a struct's fields' and a user-defined type's storage's."
  [dt]
  (tree-seq some?
            (fn [dt]
              (condp instance? dt
                ArrayType       [(.elementType ^ArrayType dt)]
                MapType         [(.keyType ^MapType dt) (.valueType ^MapType dt)]
                StructType      (map #(.dataType ^StructField %) (.fields ^StructType dt))
                UserDefinedType [(.sqlType ^UserDefinedType dt)]
                nil))
            dt))

(defn spark-conf->map [conf]
  (->> conf
       .getAll
       (map scala-tuple->vec)
       (into {})
       walk/keywordize-keys))

(defn mllib-class
  "One of MLlib's classes, such as `org.apache.spark.ml.linalg.DenseVector`.
  They come with spark-mllib, which is classic Spark only, so a Spark Connect
  client doesn't have them, and Geni looks them up when it needs them."
  ^Class [class-name]
  (or (class-named class-name)
      (throw (ex-info (str class-name " isn't on the classpath. It comes with spark-mllib, "
                           "which works with classic Spark only, not over Spark Connect.")
                      {:class class-name}))))

(def ^:private dense-vector-class (delay (class-named "org.apache.spark.ml.linalg.DenseVector")))
(def ^:private sparse-vector-class (delay (class-named "org.apache.spark.ml.linalg.SparseVector")))
(def ^:private dense-matrix-class (delay (class-named "org.apache.spark.ml.linalg.DenseMatrix")))

(defn ->dense-vector [values]
  (Reflector/invokeConstructor (mllib-class "org.apache.spark.ml.linalg.DenseVector")
                               (object-array [(double-array values)])))

(defn ->sparse-vector [size indices values]
  (Reflector/invokeConstructor (mllib-class "org.apache.spark.ml.linalg.SparseVector")
                               (object-array [(int size) (int-array indices) (double-array values)])))
(def sparse ->sparse-vector)

(defn array? [value] (.isArray (class value)))

(defn dense-vector? [value]
  (boolean (some-> ^Class @dense-vector-class (.isInstance value))))

(defn sparse-vector? [value]
  (boolean (some-> ^Class @sparse-vector-class (.isInstance value))))

(defn vector->seq
  "An MLlib vector's values, every one of them, a sparse vector's zeros too."
  [spark-vector]
  (-> spark-vector .toArray seq))

(defn- sparse-vector->seq [spark-sparse-vector]
  {:size (.size spark-sparse-vector)
   :indices (-> spark-sparse-vector .indices seq)
   :values (-> spark-sparse-vector .values seq)})

(defn matrix->seqs [matrix]
  (->> matrix .rowIter .toSeq scala-seq->vec (map vector->seq)))

(defn- spark-row->map [row]
  (let [cols   (->> row .schema .fieldNames (map keyword))
        values (->> row .toSeq scala-seq->vec (map ->clojure))]
    (zipmap cols values)))

(defn ->spark-row [x]
  (Row/fromSeq (->scala-seq x)))

(defn- map-vals->clojure
  "The map with its values through ->clojure, keeping its type, as for a
  record or a sorted map."
  [m]
  (reduce-kv (fn [acc k v]
               (let [converted (->clojure v)]
                 (if (identical? v converted) acc (assoc acc k converted))))
             m
             m))

(defn ->clojure
  "Converts a value that Spark hands back, such as a Row, a Scala collection or
  an MLlib vector, into Clojure data. A Clojure map, vector or set keeps its
  type, with its contents converted, and a Boolean comes back as `true` or
  `false` itself, which a Boolean that Java deserialised isn't."
  [value]
  (cond
    (nil? value)               nil
    (boolean? value)           (Boolean/valueOf (.booleanValue ^Boolean value))
    (map? value)               (map-vals->clojure value)
    (vector? value)            (mapv ->clojure value)
    (set? value)               (into (empty value) (map ->clojure) value)
    (coll? value)              (map ->clojure value)
    (array? value)             (map ->clojure (seq value))
    (instance? Seq value)      (map ->clojure (scala-seq->vec value))
    (instance? Iterable value) (map ->clojure (seq value))
    (instance? Map value)      (scala-map->map value)
    (instance? Row value)      (spark-row->map value)
    (dense-vector? value)      (vector->seq value)
    (sparse-vector? value)     (sparse-vector->seq value)
    (some-> ^Class @dense-matrix-class
            (.isInstance value))  (matrix->seqs value)
    (instance? Tuple2 value)   [(->clojure (._1 value)) (->clojure (._2 value))]
    (instance? Tuple3 value)   [(->clojure (._1 value))
                                (->clojure (._2 value))
                                (->clojure (._3 value))]
    :else                      value))

(defn- setter? [^java.lang.reflect.Method method]
  (and (= 1 (alength ^"[Ljava.lang.Class;" (.getParameterTypes method)))
       (re-find #"^set[A-Z]" (.getName method))))

(defn- method-keyword [^java.lang.reflect.Method method]
  (-> method
      .getName
      (replace-first #"^set" "")
      ->kebab-case
      keyword))

(defn- setters-map
  "The class's setters by param keyword, each a vector of the methods of that
  name, which can be overloads."
  [^Class cls]
  (->> cls
       .getMethods
       (filter setter?)
       (group-by method-keyword)))

(defn- setter-type [^java.lang.reflect.Method method]
  (get (.getParameterTypes method) 0))

(def ^:private number-coercions
  {Double/TYPE  double Double  double
   Float/TYPE   float  Float   float
   Long/TYPE    long   Long    long
   Integer/TYPE int    Integer int
   Short/TYPE   short  Short   short
   Byte/TYPE    byte   Byte    byte})

(defn- name-keyword [x]
  (if (keyword? x) (name x) x))

(defn ->java
  "Converts a Clojure value into an argument for a Java setter that takes
  `cls`: keywords to their names, numbers to the right width, collections to
  arrays, or to an MLlib vector, and strings to enums."
  [^Class cls value]
  (let [value  (name-keyword value)
        coerce (number-coercions cls)]
    (cond
      (and (.isAssignableFrom Seq cls)
           (.isAssignableFrom cls List))    (->scala-seq (map name-keyword value))
      (and coerce (number? value))          (coerce value)
      (and (coll? value)
           (= "org.apache.spark.ml.linalg.Vector"
              (.getName cls)))              (->dense-vector value)
      (and (.isArray cls) (coll? value))    (let [component (.getComponentType cls)
                                                  values    (vec value)
                                                  arr       (java.lang.reflect.Array/newInstance component (count values))]
                                              (dotimes [i (count values)]
                                                (java.lang.reflect.Array/set arr i (->java component (nth values i))))
                                              arr)
      (and (.isEnum cls) (string? value))   (Enum/valueOf cls ^String value)
      :else                                 value)))

(defn- set-value [^java.lang.reflect.Method method instance value]
  (.invoke method instance (into-array [(->java (setter-type method) value)])))

(defn- takes-many? [^java.lang.reflect.Method method]
  (let [^Class cls (setter-type method)]
    (or (.isArray cls) (.isAssignableFrom Seq cls))))

(defn- pick-setter
  "The one of a param's setters that suits `value`: the overload that takes an
  array or a Scala Seq for a collection, and one that doesn't otherwise, as
  with XGBoost's `setFeaturesCol`, which takes a column or several."
  [methods value]
  (or (first (filter #(= (coll? value) (takes-many? %)) methods))
      (first methods)))

(defn- edit-distance
  "The Levenshtein distance between two strings."
  [^String a ^String b]
  (peek
   (reduce (fn [previous [i x]]
             (reduce (fn [row [j y]]
                       (conj row (min (inc (peek row))
                                      (inc (nth previous (inc j)))
                                      (+ (nth previous j) (if (= x y) 0 1)))))
                     [(inc i)]
                     (map-indexed vector b)))
           (vec (range (inc (count b))))
           (map-indexed vector a))))

(defn- unknown-param! [^Class cls known k]
  (let [known   (sort (remove #{:default} known))
        closest (first (sort-by #(edit-distance (name k) (name %)) known))
        close?  (and closest
                     (<= (edit-distance (name k) (name closest))
                         (max 2 (quot (count (name k)) 3))))]
    (throw (ex-info (str (.getSimpleName cls) " has no param " k "."
                         (when close? (str " Did you mean " closest "?"))
                         " Its params are " (string/join ", " known) ".")
                    {:class cls :param k :params known}))))

(defn set-params!
  "Sets `params` on `instance` through its setters (e.g. `{:input-col
  \"text\"}` through `setInputCol`), and returns the instance. A key in
  `params` that has no setter throws, with the class's params in the message,
  before any is set."
  [instance params]
  (let [cls     (class instance)
        setters (setters-map cls)]
    (doseq [k (keys params)
            :when (not (contains? setters k))]
      (unknown-param! cls (keys setters) k))
    (doseq [[k v] params]
      (set-value (pick-setter (setters k) v) instance v))
    instance))

(defn instantiate
  "Creates an instance of `cls`, and sets `params` on it, as `set-params!`
  does."
  [^Class cls params]
  (set-params! (.newInstance cls) params))

(defmacro def-stages
  "Defines a function for each row, `[name Class]` or `[name Class doc]`, that
  makes a `package.Class` with the params given, as `instantiate` does. Spark's
  defaults hold for the rest. `:standardisation` stands for
  `:standardization`."
  [package & rows]
  `(do
     ~@(for [[fn-name cls doc] rows]
         `(defn ~fn-name ~@(when doc [doc]) [~'params]
            (instantiate ~(symbol (str package "." cls))
                         (set/rename-keys ~'params {:standardisation :standardization}))))))

(defn grid-param
  "The Param of `stage` that `k`, such as `:max-iter`, names, and `values` as
  its setter takes them, so that whole numbers suit an int param, for a param
  grid."
  [stage k values]
  (let [cls     (class stage)
        setters (setters-map cls)
        param   (first (filter #(= k (keyword (->kebab-case (.name %)))) (.params stage)))]
    (when-not (and param (setters k))
      (unknown-param! cls (keys setters) k))
    [param (mapv #(->java (setter-type (pick-setter (setters k) %)) %) values)]))

(defn dense [& values]
  (let [flattened (mapcat ensure-coll values)]
    (->dense-vector flattened)))

(defn row [& values]
  (->spark-row values))

(docs/add-doc!
 (var dense)
 (-> docs/spark-docs :methods :ml :linalg :vectors :dense))

(docs/add-doc!
 (var sparse)
 (-> docs/spark-docs :methods :ml :linalg :vectors :sparse))

(docs/add-doc!
 (var row)
 (-> docs/spark-docs :methods :core :row :from-seq))
