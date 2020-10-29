(ns zero-one.geni.arrow
  (:import [org.apache.arrow.vector VarCharVector
            VectorSchemaRoot BigIntVector Float4Vector Float8Vector IntVector
            BitVector DateMilliVector ValueVector BaseFixedWidthVector TimeStampMilliVector]
           [org.apache.arrow.vector.util Text]
           [org.apache.arrow.vector.ipc ArrowStreamWriter]
           [org.apache.arrow.memory RootAllocator]
           [java.nio.channels Channels]
           [org.apache.spark.sql Row]
           [java.sql Date]
           [org.apache.arrow.vector.types.pojo Schema]
           [scala.collection Iterator]
           [scala.collection.convert Wrappers$IteratorWrapper]
           )
  (:require
   [clojure.java.io :as io]
   [zero-one.geni.interop :as interop]

   ))

;; (set! *warn-on-reflection* true)


;;;  these protocolls are just needed to make type hints working for the typed-action function
(defprotocol IValueInfo
  (^BaseFixedWidthVector vector [this])
  (^long idx1 [this])
  (^Object value [this])
  )

(defprotocol IRowInfo
  (^Row row [this])
  (^long idx2 [this])
  )

(defrecord ValueInfo [^BaseFixedWidthVector a-v ^long a-idx ^Object a-value]
  IValueInfo
  (vector ^BaseFixedWidthVector [this] a-v)
  (idx1 ^long [this] a-idx)
  (value ^Object [this] a-value)

  )



(defrecord RowInfo [^Row row ^long idx]
  IRowInfo
  (row ^Row [this] row )
  (idx2 ^long [this] idx)
  )

;;; this method will be called for evey value transformed, so relection shoudkl be avoided
(defn typed-action [action type value-info row-info ^String name ^RootAllocator allocator]
  (when (= :set type)
    (if (nil? (value value-info))
      (throw (Exception. "value cannot be null"))
      ))
  (case type
    :string (case action
              :set (.setSafe ^VarCharVector (vector value-info) (idx1 value-info) (Text. ^String (value value-info)))
              :make-vector (VarCharVector. name allocator)
              :get (.getString (row row-info) (idx2 row-info)))

    :double (case action
              :set (.set ^Float8Vector (vector value-info) (idx1 value-info) ^double (value value-info))
              :make-vector (Float8Vector. name allocator)
              :get (if (.isNullAt (row row-info) (idx2 row-info)) nil (.getDouble (row row-info) (idx2 row-info))))

    :float (case action
             :set (.set ^Float4Vector (vector value-info) (idx1 value-info) ^double (value value-info))
             :make-vector (Float4Vector. name allocator)
             :get (if (.isNullAt (row row-info) (idx2 row-info)) nil (.getFloat (row row-info) (idx2 row-info))))
    :long (case action
            :set (.set ^BigIntVector (vector value-info) (idx1 value-info) ^long (value value-info))
            :make-vector (BigIntVector. name allocator)
            :get (.getLong (row row-info) (idx2 row-info)))


    :integer (case action
               :set (.set ^IntVector (vector value-info) (idx1 value-info) ^int (value value-info))
               :make-vector (IntVector. name allocator)
               :get (if (.isNullAt (row row-info) (idx2 row-info)) nil (.getInt (row row-info) (idx2 row-info))))

    :boolean (case action
               :set (.set ^BitVector (vector value-info) (idx1 value-info) 1 (if (value value-info) 1 0))
               :make-vector (BitVector. name allocator)
               :get (if (.isNullAt (row row-info) (idx2 row-info)) nil (.getBoolean (row row-info) (idx2 row-info))))

    :date
    (case action
      :set (.set ^TimeStampMilliVector (vector value-info) (idx1 value-info) (.getTime ^java.sql.Date (value value-info)))
      :make-vector (TimeStampMilliVector. name allocator)
      :get (if (.isNullAt (row row-info) (idx2 row-info)) nil (.getDate (row row-info) (idx2 row-info)))))
  )


(defn typed-set [v  idx value type]
  (typed-action :set type (->ValueInfo v idx value) nil nil nil))

(defn typed-make-vector [name allocator type]
  (typed-action :make-vector type nil nil name allocator))

(defn typed-get [row idx type]
  (typed-action :get type nil (->RowInfo row idx) nil nil))


(defn schema->clojure [^Schema schema]
  (map
   #(hash-map
     :type %1
     :name %2)
   (->> schema .fields (map #(keyword (.. % dataType typeName))))
   (->> schema .fields (map #(.. % name)))))

(defn set-null-or-value [v ^long idx value type]
  (if (nil? value)
    (.setNull v idx)
    (typed-set v idx value type)))

(defn fill-vector [vector values type]
  (let [_ (.allocateNew vector (count values))
        idx-vals (map #(hash-map :idx %1 :value %2)
                      (range)
                      values)
        _ (run! #(set-null-or-value vector (:idx  %) (:value %) type)
                idx-vals )
        _ (.setValueCount ^ValueVector vector (count values))]


    vector))


(defn rows->data [rows schema-maps]
  (let []
    (partition
     (count schema-maps)
     (for [row rows field-idx (range (count schema-maps))]
       (let [name (:name  (nth schema-maps field-idx))
             type (:type (nth schema-maps field-idx))]
         (typed-get row field-idx type))))))


(defn rows->vectors [rows schema-maps]
(let [allocator (RootAllocator. Long/MAX_VALUE)
          schema-names (map :name schema-maps)
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

(defn export-rows! [row-num rows schema-maps out-dir]
  (when (pos? (count rows))
    (let [
          vectors (rows->vectors rows schema-maps)
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

(defn collect-to-arrow  [rdd chunk-size out-dir]
  "Collects the dataframe on driver and exports it as arrow files.

 The data gets transfered by partition, and so each partions should be small enough to
 fit in heap space of the driver. Then the data is saved in chunks of `chunk-size` rows to disk as arrow files.


 `rdd` Spark dataset
 `chunk-size` Number of rows each arrow file will have. Should be small enoungh to make data fit in heap space of driver.
 `out-dir` Output dir of arrow files

"

  (let [first-row (.first rdd)
        schema (schema->clojure (.schema first-row))
        ^Wrappers$IteratorWrapper row-iterator (.toLocalIterator rdd)]
    (loop [acc [] files [] counter 0 glob-counter 0]

      (let [has-next (.hasNext row-iterator)]
        (cond (not has-next) (conj files (export-rows! glob-counter acc schema out-dir))
              (= counter chunk-size)
              (recur []
                     (conj files (export-rows! glob-counter acc schema out-dir))
                     0 glob-counter)
              true (recur  (conj acc (.next row-iterator))
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
   (g/collect-to-arrow housing 3000 "/tmp/arrow-out"))


  )
