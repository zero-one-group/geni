(ns zero-one.geni.scala
  (:import
    (java.io ByteArrayOutputStream)
    (org.apache.spark.ml.linalg Vectors)
    (scala Console Function0)
    (scala.collection JavaConversions Map)))

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

