(ns zero-one.geni.arrow
  (:require
   [clojure.java.io :as io])
  (:import
   (java.nio.channels Channels)
   (org.apache.arrow.memory RootAllocator)
   (org.apache.arrow.vector BaseFixedWidthVector
                            BigIntVector
                            BitVector
                            Float4Vector
                            Float8Vector
                            IntVector
                            TimeStampMilliVector
                            ValueVector
                            VarCharVector
                            VectorSchemaRoot)
   (org.apache.arrow.vector.ipc ArrowStreamWriter)
   (org.apache.arrow.vector.types.pojo Schema)
   (org.apache.arrow.vector.util Text)
   (org.apache.spark.sql Row)
   (scala.collection.convert Wrappers$IteratorWrapper)))

(defn typed-action [action
                    col-type
                    value-info
                    row-info
                    ^String col-name
                    ^RootAllocator allocator]
  (let [value (:value value-info)
        ^BaseFixedWidthVector arrow-vector (:vector value-info)
        ^int idx-value (:idx value-info)
        ^Row row (:row row-info)
        ^long idx-row (:idx row-info)]
    (case col-type

      :string
      (case action
        :set-null    (.setNull ^VarCharVector arrow-vector idx-value)
        :set         (.setSafe ^VarCharVector arrow-vector idx-value (Text. ^String value))
        :make-vector (VarCharVector. col-name allocator)
        :get         (.getString row idx-row))

      :double
      (case action
        :set-null    (.setNull ^BaseFixedWidthVector arrow-vector idx-value)
        :set         (.set ^Float8Vector arrow-vector idx-value ^float value)
        :make-vector (Float8Vector. col-name allocator)
        :get         (when-not (.isNullAt row idx-row) (.getDouble row idx-row)))

      :float
      (case action
        :set-null    (.setNull ^BaseFixedWidthVector arrow-vector idx-value)
        :set         (.set ^Float4Vector arrow-vector idx-value  ^double value)
        :make-vector (Float4Vector. col-name allocator)
        :get         (when-not (.isNullAt row idx-row) (.getFloat row idx-row)))

      :long
      (case action
        :set-null    (.setNull ^BaseFixedWidthVector arrow-vector idx-value)
        :set         (.set ^BigIntVector arrow-vector idx-value ^long value)
        :make-vector (BigIntVector. col-name allocator)
        :get         (when-not (.isNullAt row idx-row) (.getLong row idx-row)))

      :integer
      (case action
        :set-null    (.setNull ^BaseFixedWidthVector arrow-vector idx-value)
        :set         (.set ^IntVector arrow-vector idx-value ^int value)
        :make-vector (IntVector. col-name allocator)
        :get         (when-not (.isNullAt row idx-row) (.getInt row idx-row)))

      :boolean
      (case action
        :set-null    (.setNull ^BaseFixedWidthVector arrow-vector idx-value)
        :set         (.set ^BitVector arrow-vector idx-value 1 (if value 1 0))
        :make-vector (BitVector. col-name allocator)
        :get         (when-not (.isNullAt row idx-row) (.getBoolean row idx-row)))

      :date
      (case action
        :set-null    (.setNull ^BaseFixedWidthVector arrow-vector idx-value)
        :set         (.set ^TimeStampMilliVector arrow-vector idx-value (.getTime ^java.sql.Date value))
        :make-vector (TimeStampMilliVector. col-name allocator)
        :get         (when-not (.isNullAt row idx-row) (.getDate row idx-row))))))

(defn- typed-set [arrow-vector idx value col-type]
  (typed-action :set col-type {:vector arrow-vector :idx idx :value value} nil nil nil))

(defn- typed-make-vector [col-name allocator col-type]
  (typed-action :make-vector col-type nil nil col-name allocator))

(defn- typed-get [row idx col-type]
  (typed-action :get col-type nil {:row row :idx idx} nil nil))

(defn- schema->clojure [^Schema schema]
  (let [fields (.fields schema)
        types (map #(keyword (.. % dataType typeName)) fields)
        names (map #(.name %) fields)]
    (map
     (fn [col-type col-name]
       (hash-map :type col-type :name col-name))
     types
     names)))

(defn- set-null-or-value [arrow-vector ^long idx value col-type]
  (if (nil? value)
    (typed-action :set-null col-type {:vector arrow-vector :idx 0 :value nil} nil nil nil)
    (typed-set arrow-vector idx value col-type)))

(defn- fill-vector! [arrow-vector values col-type]
  (let [n-values (count values)
        idxs     (long-array (range n-values))
        arr-vals (to-array values)]
    (.allocateNew arrow-vector n-values)
    (run! #(set-null-or-value arrow-vector ^long % (aget arr-vals ^long %) col-type) idxs)
    (.setValueCount ^ValueVector arrow-vector (count values))
    arrow-vector))

(defn- rows->data [rows schema-maps]
  (for [row rows]
    (for [[col-idx schema] (map vector (range) schema-maps)]
      (typed-get row col-idx (:type schema)))))

(defn- rows->vectors [rows schema-maps]
  (let [allocator  (RootAllocator. Long/MAX_VALUE)
        data       (rows->data rows schema-maps)
        transposed (if (seq data) (apply pmap list data) [])
        vectors    (pmap (fn [schema values]
                           (let [col-name     (:name schema)
                                 col-type     (:type schema)
                                 arrow-vector (typed-make-vector col-name
                                                                 allocator
                                                                 col-type)]
                             (fill-vector! arrow-vector values col-type)))
                         schema-maps
                         transposed)]
    vectors))

(defn- export-rows! [rows schema-maps out-dir]
  (let [vectors  (rows->vectors rows schema-maps)
        fields   (map #(.getField ^ValueVector %) vectors)
        root     (VectorSchemaRoot. fields vectors)
        out-file (java.io.File/createTempFile "geni" ".ipc" (io/file out-dir))]
    (with-open [out    (Channels/newChannel (clojure.java.io/output-stream out-file))
                writer (ArrowStreamWriter. root nil out)]
      (doto writer
        (.start)
        (.writeBatch)
        (.end)))
    (.getPath out-file)))

(defn collect-to-arrow
 "Collects the dataframe on driver and exports it as arrow files.
  The data gets transfered by partition, and so each partions should be small
   enough to fit in heap space of the driver. Then the data is saved in chunks
   of `chunk-size` rows to disk as arrow files.

   `rdd` Spark dataset
   `chunk-size` Number of rows each arrow file will have. Should be small
    enoungh to make data fit in heap space of driver.
   `out-dir` Output dir of arrow files"
  [rdd chunk-size out-dir]
  (let [schema (schema->clojure (.schema rdd))
        ^Wrappers$IteratorWrapper row-iterator (.toLocalIterator rdd)]
    (loop [acc          []
           files        []
           counter      0
           glob-counter 0]
      (let [has-next (.hasNext row-iterator)]
        (cond (not has-next)
              (conj files (export-rows! acc schema out-dir))
              (= counter chunk-size)
              (recur []
                     (conj files (export-rows! acc schema out-dir))
                     0 glob-counter)
              :else
              (recur (conj acc (.next row-iterator))
                     files
                     (inc counter)
                     (inc glob-counter)))))))
