(ns zero-one.geni.interop
  (:require
   [clojure.string :as string :refer [replace-first]]
   [clojure.walk :as walk]
   [zero-one.geni.docs :as docs]
   [zero-one.geni.utils :refer [->kebab-case class-named ensure-coll]])
  (:import
   (clojure.lang Reflector)
   (java.io ByteArrayOutputStream PrintStream)
   (org.apache.spark.sql Row)
   (scala Console
          Function0
          Function1
          Function2
          Function3
          Tuple2
          Tuple3)
   (scala.collection JavaConverters Map Seq)
   (scala.collection.immutable List)))

(declare ->clojure)

(defn ->java-list [coll]
  (java.util.ArrayList. coll))

(defn scala-seq? [value]
  (instance? Seq value))

(defn iterable? [value]
  (instance? Iterable value))

(defn scala-map? [value]
  (instance? Map value))

(defn scala-tuple2? [value]
  (instance? Tuple2 value))

(defn scala-tuple3? [value]
  (instance? Tuple3 value))

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

(defn spark-row? [value]
  (instance? Row value))

(defn dense-vector? [value]
  (boolean (some-> ^Class @dense-vector-class (.isInstance value))))

(defn sparse-vector? [value]
  (boolean (some-> ^Class @sparse-vector-class (.isInstance value))))

(defn dense-matrix? [value]
  (boolean (some-> ^Class @dense-matrix-class (.isInstance value))))

(defn vector->seq [spark-vector]
  (-> spark-vector .values seq))

(defn sparse-vector->seq [spark-sparse-vector]
  {:size (.size spark-sparse-vector)
   :indices (-> spark-sparse-vector .indices seq)
   :values (-> spark-sparse-vector .values seq)})

(defn matrix->seqs [matrix]
  (->> matrix .rowIter .toSeq scala-seq->vec (map vector->seq)))

(defn spark-row->map [row]
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
    (nil? value)            nil
    (boolean? value)        (Boolean/valueOf (.booleanValue ^Boolean value))
    (map? value)            (map-vals->clojure value)
    (vector? value)         (mapv ->clojure value)
    (set? value)            (into (empty value) (map ->clojure) value)
    (coll? value)           (map ->clojure value)
    (array? value)          (map ->clojure (seq value))
    (scala-seq? value)      (map ->clojure (scala-seq->vec value))
    (iterable? value)       (map ->clojure (seq value))
    (scala-map? value)      (scala-map->map value)
    (spark-row? value)      (spark-row->map value)
    (dense-vector? value)   (vector->seq value)
    (sparse-vector? value)  (sparse-vector->seq value)
    (dense-matrix? value)   (matrix->seqs value)
    (scala-tuple2? value)   [(->clojure (._1 value)) (->clojure (._2 value))]
    (scala-tuple3? value)   [(->clojure (._1 value))
                             (->clojure (._2 value))
                             (->clojure (._3 value))]
    :else                   value))

(defn setter? [^java.lang.reflect.Method method]
  (and (= 1 (alength ^"[Ljava.lang.Class;" (.getParameterTypes method)))
       (re-find #"^set[A-Z]" (.getName method))))

(defn method-keyword [^java.lang.reflect.Method method]
  (-> method
      .getName
      (replace-first #"set" "")
      ->kebab-case
      keyword))

(defn setters-map
  "The class's setters by param keyword, each a vector of the methods of that
  name, which can be overloads."
  [^Class cls]
  (->> cls
       .getMethods
       (filter setter?)
       (group-by method-keyword)))

(defn setter-type [^java.lang.reflect.Method method]
  (get (.getParameterTypes method) 0))

(def ^:private number-coercions
  {Double/TYPE  double Double  double
   Float/TYPE   float  Float   float
   Long/TYPE    long   Long    long
   Integer/TYPE int    Integer int
   Short/TYPE   short  Short   short
   Byte/TYPE    byte   Byte    byte})

(defn ->java
  "Converts a Clojure value into an argument for a Java setter that takes
  `cls`: numbers to the right width, collections to arrays, strings to enums."
  [^Class cls value]
  (let [coerce (number-coercions cls)]
    (cond
      (and (.isAssignableFrom Seq cls)
           (.isAssignableFrom cls List))    (->scala-seq value)
      (and coerce (number? value))          (coerce value)
      (and (.isArray cls) (coll? value))    (let [component (.getComponentType cls)
                                                  values    (vec value)
                                                  arr       (java.lang.reflect.Array/newInstance component (count values))]
                                              (dotimes [i (count values)]
                                                (java.lang.reflect.Array/set arr i (->java component (nth values i))))
                                              arr)
      (and (.isEnum cls) (string? value))   (Enum/valueOf cls ^String value)
      :else                                 value)))

(defn set-value [^java.lang.reflect.Method method instance value]
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

(defn convert-keywords [value]
  (cond
    (keyword? value)              (name value)
    (and (coll? value)
         (every? keyword? value)) (map name value)
    :else                         value))

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

(defn- unknown-param! [^Class cls setters k]
  (let [known   (sort (remove #{:default} (keys setters)))
        closest (first (sort-by #(edit-distance (name k) (name %)) known))
        close?  (and closest
                     (<= (edit-distance (name k) (name closest))
                         (max 2 (quot (count (name k)) 3))))]
    (throw (ex-info (str (.getSimpleName cls) " has no param " k "."
                         (when close? (str " Did you mean " closest "?"))
                         " Its params are " (string/join ", " known) ".")
                    {:class cls :param k :params known}))))

(defn instantiate
  "Creates an instance of `cls`, sets `params` through its setters (e.g.
  `{:input-col \"text\"}` through `setInputCol`), and returns the instance. A
  key in `params` that has no setter throws, with the class's params in the
  message. The keys in `defaults`, which are Geni's own, are set when the class
  has a setter for them and skipped when it doesn't, since a default can be
  missing from one Spark version."
  ([cls params] (instantiate cls {} params))
  ([^Class cls defaults params]
   (let [setters  (setters-map cls)
         instance (.newInstance cls)]
     (doseq [k (keys params)
             :when (not (contains? setters k))]
       (unknown-param! cls setters k))
     (doseq [[k v] (merge defaults params)
             :let  [v       (convert-keywords v)
                    methods (setters k)]
             :when methods]
       (set-value (pick-setter methods v) instance v))
     instance)))

(defn zero-arity? [^java.lang.reflect.Method method]
  (= 0 (alength ^"[Ljava.lang.Class;" (.getParameterTypes method))))

(defn fields-map [^Class cls]
  (->> cls
       .getMethods
       (filter zero-arity?)
       (map #(vector (method-keyword %) %))
       (into {})))

(defn get-field [instance field-keyword]
  (let [fields (fields-map (class instance))]
    (.invoke (fields field-keyword) instance (into-array []))))

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
