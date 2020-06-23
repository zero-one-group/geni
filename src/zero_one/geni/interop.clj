(ns zero-one.geni.interop
  (:require
    [camel-snake-kebab.core :refer [->kebab-case]]
    [clojure.java.data :as j]
    [clojure.string :refer [replace-first]]
    [zero-one.geni.utils :refer [vector-of-numbers?]])
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
           Function2)
    (scala.collection JavaConversions Map Seq)))

(defn scala-seq? [value]
  (instance? Seq value))

(defn scala-seq->vec [scala-seq]
  (vec (JavaConversions/seqAsJavaList scala-seq)))

(defn scala-map->map [^Map m]
  (into {} (JavaConversions/mapAsJavaMap m)))

(defn ->scala-seq [coll]
  (JavaConversions/asScalaBuffer (seq coll)))

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

(defmacro with-scala-out-str [& body]
  `(let [out-buffer# (ByteArrayOutputStream.)]
      (Console/withOut
        out-buffer#
        (->scala-function0 (fn [] ~@body)))
      (.toString out-buffer# "UTF-8")))

(defn ->scala-coll [value]
  (cond
    (vector-of-numbers? value) (let [[x & xs] value] (Vectors/dense x (->scala-seq xs)))
    (coll? value) (->scala-seq value)
    :else value))

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

(defn matrix->seqs [matrix]
  (->> matrix .rowIter .toSeq scala-seq->vec (map vector->seq)))

(declare ->clojure)
(defn spark-row->map [row]
  (let [cols   (->> row .schema .fieldNames (map keyword))
        values (->> row .toSeq scala-seq->vec (map ->clojure))]
    (zipmap cols values)))

(defn ->clojure [value]
  (cond
    (nil? value)            nil
    (coll? value)           (map ->clojure value)
    (array? value)          (map ->clojure (seq value))
    (scala-seq? value)      (map ->clojure (scala-seq->vec value))
    (spark-row? value)      (spark-row->map value)
    (dense-vector? value)   (vector->seq value)
    (sparse-vector? value)  (vector->seq value)
    (dense-matrix? value)   (matrix->seqs value)
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
