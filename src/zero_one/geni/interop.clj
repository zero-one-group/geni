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
    (scala Console Function0)
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
       (clojure.core/map #(.productElement p %))
       (into [])))

(defn ->scala-function0 [f]
  (reify Function0 (apply [this] (f))))

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

(defn spark-row->vec [row]
  (-> row .toSeq scala-seq->vec))

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

(defn ->clojure [value]
  (cond
    (nil? value)            nil
    (coll? value)           (map ->clojure value)
    (array? value)          (map ->clojure (seq value))
    (scala-seq? value)      (map ->clojure (scala-seq->vec value))
    (spark-row? value)      (spark-row->vec value)
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

(defn instantiate
  ([^Class cls props]
   (let [setters  (setters-map cls)
         instance (.newInstance cls)]
     (reduce
       (fn [_ [k v]]
         (when-let [setter (setters k)]
           (set-value setter instance v)))
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
