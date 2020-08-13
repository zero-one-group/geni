(ns zero-one.geni.rdd.kryo
  (:import
    [java.nio ByteBuffer]
    [org.apache.spark SparkEnv]
    [org.apache.spark.serializer SerializerInstance]
    [scala.reflect ClassTag$]))

(set! *warn-on-reflection* true)

(def ^:no-doc OBJECT-CLASS-TAG
  (.apply ClassTag$/MODULE$ java.lang.Object))

(defn ^bytes serialize [^Object obj]
  (let [^SerializerInstance ser (.. (SparkEnv/get) serializer newInstance)
        ^ByteBuffer buf (.serialize ser obj OBJECT-CLASS-TAG)]
    (.array buf)))

(defn deserialize [^bytes b]
  (let [^ByteBuffer buf (ByteBuffer/wrap b)
        ^SerializerInstance ser (.. (SparkEnv/get) serializer newInstance)]
    (.deserialize ser buf OBJECT-CLASS-TAG)))
