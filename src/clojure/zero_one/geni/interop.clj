(ns zero-one.geni.interop
  (:require
   [camel-snake-kebab.core :refer [->kebab-case]]
   [clojure.java.data :as j]
   [clojure.string :refer [replace-first]]
   [clojure.walk :as walk]
   [clojure.reflect :refer [resolve-class]]
   [zero-one.geni.docs :as docs]
   [zero-one.geni.utils :refer [ensure-coll]])
  (:import
   (java.io ByteArrayOutputStream)
   (org.apache.spark.ml.linalg DenseVector
                               DenseMatrix
                               SparseVector
                               Vectors)
   (org.apache.spark.sql Row)
   (scala Console
          Function0
          Function1
          Function2
          Function3
          Tuple2
          Tuple3)
   (scala.collection Map Seq)
   (scala.collection.convert Wrappers$IterableWrapper)
   (scala.collection JavaConverters)))

(declare ->clojure)

(defn ->java-list [coll]
  (java.util.ArrayList. coll))

(defn scala-seq? [value]
  (instance? Seq value))

(defn iterable? [value]
  (instance? Wrappers$IterableWrapper value))

(defn scala-map? [value]
  (instance? Map value))

(defn scala-tuple2? [value]
  (instance? Tuple2 value))

(defn scala-tuple3? [value]
  (instance? Tuple3 value))

(defn scala-seq->vec [scala-seq]
  (-> scala-seq (JavaConverters/seqAsJavaList) vec))

(defn scala-map->map [^Map m]
  (into {}
        (for [[k v] (JavaConverters/mapAsJavaMap m)]
          [k (->clojure v)])))

(defn ->scala-seq [coll]
  (JavaConverters/asScalaBuffer (seq coll)))

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

(defn ->scala-map
  [m]
  (JavaConverters/mapAsScalaMap m))

(defn class-exists? [c]
  (resolve-class (.getContextClassLoader (Thread/currentThread)) c))

(defn optional->nillable [value]
  (when (.isPresent value)
    (.get value)))

(defmacro with-scala-out-str [& body]
  `(let [out-buffer# (ByteArrayOutputStream.)]
     (Console/withOut
      out-buffer#
      (->scala-function0 (fn [] ~@body)))
     (.toString out-buffer# "UTF-8")))

(defn spark-conf->map [conf]
  (->> conf
       .getAll
       (map scala-tuple->vec)
       (into {})
       walk/keywordize-keys))

(defn ->dense-vector [values]
  (let [[x & xs] values]
    (Vectors/dense x (->scala-seq xs))))

(defn ->sparse-vector [size indices values]
  (SparseVector. size (int-array indices) (double-array values)))
(def sparse ->sparse-vector)

(defn array? [value] (.isArray (class value)))

(defn spark-row? [value]
  (instance? Row value))

(defn dense-vector? [value]
  (instance? DenseVector value))

(defn sparse-vector? [value]
  (instance? SparseVector value))

(defn dense-matrix? [value]
  (instance? DenseMatrix value))

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

(defn ->clojure [value]
  (cond
    (nil? value)            nil
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

(defn setters-map [^Class cls]
  (->> cls
       .getMethods
       (filter setter?)
       (map #(vector (method-keyword %) %))
       (into {})))

(defn setter-type [^java.lang.reflect.Method method]
  (get (.getParameterTypes method) 0))

(defn ->java [^Class cls value]
  (if (= cls scala.collection.Seq)
    (->scala-seq value)
    (j/to-java cls value)))

(defn set-value [^java.lang.reflect.Method method instance value]
  (.invoke method instance (into-array [(->java (setter-type method) value)])))

(defn convert-keywords [value]
  (cond
    (keyword? value)              (name value)
    (and (coll? value)
         (every? keyword? value)) (map name value)
    :else                         value))

(defn instantiate
  ([^Class cls props]
   (let [setters  (setters-map cls)
         instance (.newInstance cls)]
     (reduce
      (fn [_ [k v]]
        (when-let [setter (setters k)]
          (set-value setter instance (convert-keywords v))))
      instance
      props))))

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
