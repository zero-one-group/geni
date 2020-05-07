(ns zero-one.geni.interop
  (:require
    [camel-snake-kebab.core :refer [->kebab-case]]
    [clojure.java.data :as j]
    [clojure.string :refer [replace-first]])
  (:import
    (java.io ByteArrayOutputStream)
    (org.apache.spark.ml.linalg DenseVector Vectors)
    (scala Console Function0)
    (scala.collection JavaConversions Map Seq)))

(defn scala-seq->vec [scala-seq]
  (into [] (JavaConversions/seqAsJavaList scala-seq)))

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
  (if (vector? value)
    (let [[head & tail] value]
      (Vectors/dense head (->scala-seq tail)))
    value))

(defn array? [value] (.isArray (class value)))

(defn dense-vector? [value]
  (instance? DenseVector value))

(defn vector->seq [spark-vector]
  (-> spark-vector .values seq))

(defn matrix->seqs [matrix]
  (let [n-cols (.numCols matrix)]
    (->> matrix .values seq (partition n-cols))))

(defn ->clojure [value]
  (cond
    (array? value)         (seq value)
    (dense-vector? value)  (vector->seq value)
    :else                  value))

(defn setter? [^java.lang.reflect.Method method]
  (and (= 1 (alength ^"[Ljava.lang.Class;" (.getParameterTypes method)))
       (re-find #"^set[A-Z]" (.getName method))))

(defn setter-keyword [^java.lang.reflect.Method method]
  (-> method
      .getName
      (replace-first #"set" "")
      ->kebab-case
      keyword))

(defn setters-map [^Class cls]
  (->> cls
       .getMethods
       (filter setter?)
       (map #(vector (setter-keyword %) %))
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
