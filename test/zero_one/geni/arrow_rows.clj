(ns zero-one.geni.arrow-rows
  "Reads Arrow IPC streams back with Arrow itself, for the tests of
  `g/to-arrow`. Only classic tests load it, since it needs Apache Arrow."
  (:import
   (java.io ByteArrayInputStream)
   (org.apache.arrow.memory RootAllocator)
   (org.apache.arrow.vector FieldVector VectorSchemaRoot)
   (org.apache.arrow.vector.ipc ArrowStreamReader)
   (org.apache.arrow.vector.util Text)))

(defn read-rows
  "The rows in an Arrow IPC stream, as vectors of their values."
  [^bytes stream]
  (with-open [allocator (RootAllocator.)
              reader    (ArrowStreamReader. (ByteArrayInputStream. stream) allocator)]
    (let [^VectorSchemaRoot root (.getVectorSchemaRoot reader)]
      (loop [rows []]
        (if (.loadNextBatch reader)
          (recur (into rows
                       (for [i (range (.getRowCount root))]
                         (mapv (fn [^FieldVector v]
                                 (let [value (.getObject v (int i))]
                                   (if (instance? Text value) (str value) value)))
                               (.getFieldVectors root)))))
          rows)))))
