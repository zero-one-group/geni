(ns zero-one.geni.arrow
  (:require [clojure.java.io :as io])
  (:import java.nio.channels.Channels
           org.apache.arrow.memory.RootAllocator
           [org.apache.arrow.vector BaseFixedWidthVector BigIntVector BitVector Float4Vector Float8Vector IntVector
            TimeStampMilliVector ValueVector VarCharVector VectorSchemaRoot]
           org.apache.arrow.vector.ipc.ArrowStreamWriter
           org.apache.arrow.vector.types.pojo.Schema
           org.apache.arrow.vector.util.Text
           org.apache.spark.sql.Row
           scala.collection.convert.Wrappers$IteratorWrapper))

 ;; (set! *warn-on-reflection* true)


(defn- typed-action [action type value-info row-info ^String name ^RootAllocator allocator]
  (let [value (:value value-info)
        ^BaseFixedWidthVector vector (:vector value-info)
        ^long idx-value (:idx value-info)
        ^Row row (:row row-info)
        ^long idx-row (:idx row-info)
        ]
   (case type

     :string (case action
               :set (.setSafe ^VarCharVector vector idx-value (Text. ^String value))
               :make-vector (VarCharVector. name allocator)
               :get (.getString row idx-row))

     :double (case action
               :set (.set ^Float8Vector vector idx-value ^float value)
               :make-vector (Float8Vector. name allocator)
               :get (if (.isNullAt row idx-row) nil (.getDouble row idx-row)))

     :float (case action
              :set (.set ^Float4Vector vector idx-value  ^double value)
              :make-vector (Float4Vector. name allocator)
              :get (if (.isNullAt row idx-row) nil (.getFloat row idx-row)))
     :long (case action
             :set (.set ^BigIntVector vector idx-value ^long value)
             :make-vector (BigIntVector. name allocator)
             :get (if (.isNullAt row idx-row) nil (.getLong row idx-row)))


     :integer (case action
                :set (.set ^IntVector vector idx-value ^int value)
                :make-vector (IntVector. name allocator)
                :get (if (.isNullAt row idx-row) nil (.getInt row idx-row)))

     :boolean (case action
                :set (.set ^BitVector vector idx-value 1 (if value 1 0))
                :make-vector (BitVector. name allocator)
                :get (if (.isNullAt row idx-row) nil (.getBoolean row idx-row)))

     :date (case action
             :set (.set ^TimeStampMilliVector vector idx-value (.getTime ^java.sql.Date value))
             :make-vector (TimeStampMilliVector. name allocator)
             :get (if (.isNullAt row idx-row) nil (.getDate row idx-row))))))


(defn- typed-set [v  idx value type]
  (typed-action :set type {:vector v :idx idx :value value} nil nil nil))

(defn- typed-make-vector [name allocator type]
  (typed-action :make-vector type nil nil name allocator))

(defn- typed-get [row idx type]
  (typed-action :get type nil {:row row :idx idx} nil nil))


(defn- schema->clojure [^Schema schema]
  (let [fields (.fields schema)
        types (map #(keyword (.. % dataType typeName)) fields)
        names (map #(.name %) fields)]
    (map
     (fn [type name]
       (hash-map :type type :name name))
     types
     names
     )))

(defn- set-null-or-value [v ^long idx value type]
  (if (nil? value)
    (.setNull v idx)
    (typed-set v idx value type)))

(defn- fill-vector [vector values type]
  (let [_ (.allocateNew vector (count values))
        idx-vals (map #(hash-map :idx %1 :value %2)
                      (range)
                      values)
        _ (run! #(set-null-or-value vector (:idx  %) (:value %) type)
                idx-vals )
        _ (.setValueCount ^ValueVector vector (count values))]


    vector))


(defn- rows->data [rows schema-maps]
  (partition
   (count schema-maps)
   (for [row rows field-idx (range (count schema-maps))]
     (let [type (:type (nth schema-maps field-idx))]
       (typed-get row field-idx type)))))


(defn- rows->vectors [rows schema-maps]
(let [allocator (RootAllocator. Long/MAX_VALUE)
          data (rows->data rows schema-maps)
          transposed-data (apply pmap list data)
          vectors (pmap
                   #(fill-vector
                     (typed-make-vector
                      (:name %1)
                      allocator
                      (:type % 1))
                     %2
                     (:type %1))
                   schema-maps
                   transposed-data)
          ]
      vectors
      )

  )

(defn- export-rows! [rows schema-maps out-dir]
  (when (pos? (count rows))
    (let [vectors (rows->vectors rows schema-maps)
          vector-fields (map #(.getField ^ValueVector %) vectors)
          root (VectorSchemaRoot. vector-fields vectors)
          out-file (java.io.File/createTempFile "geni" ".ipc" (io/file out-dir))]

      (with-open [out (Channels/newChannel  (clojure.java.io/output-stream out-file))
                  writer  (ArrowStreamWriter. root nil out)
                  ]
        (doto writer
          (.start)
          (.writeBatch)
          (.end)))

      (.getPath out-file))

    ))

(defn collect-to-arrow
  "Collects the dataframe on driver and exports it as arrow files.

 The data gets transfered by partition, and so each partions should be small enough to
 fit in heap space of the driver. Then the data is saved in chunks of `chunk-size` rows to disk as arrow files.


 `rdd` Spark dataset
 `chunk-size` Number of rows each arrow file will have. Should be small enoungh to make data fit in heap space of driver.
 `out-dir` Output dir of arrow files

"
  [rdd chunk-size out-dir]
  (let [first-row (.first rdd)
        schema (schema->clojure (.schema first-row))
        ^Wrappers$IteratorWrapper row-iterator (.toLocalIterator rdd)]
    (loop [acc [] files [] counter 0 glob-counter 0]

      (let [has-next (.hasNext row-iterator)]
        (cond (not has-next) (conj files (export-rows! acc schema out-dir))
              (= counter chunk-size)
              (recur []
                     (conj files (export-rows! acc schema out-dir))
                     0 glob-counter)
              :else (recur  (conj acc (.next row-iterator))
                            files
                            (inc counter)
                            (inc glob-counter)))))))


(comment
  (require '[zero-one.geni.core :as g])
  (require '[tech.v3.dataset :as ds])
  (require '[tech.v3.libs.arrow :as ds-arrow])

  (def housing
    (g/cache
     (g/read-parquet! "test/resources/melbourne_housing_snapshot.parquet")))

  (g/shape housing)
  (time
   (g/collect-to-arrow housing 3000 "/tmp/arrow-out")))
