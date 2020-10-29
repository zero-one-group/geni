;; Docstring Sources:
;; https://numpy.org/doc/
;; https://pandas.pydata.org/docs/
(ns zero-one.geni.core.foreign-idioms
  (:require
   [clojure.string :as string]
   [potemkin :refer [import-fn]]
   [zero-one.geni.core.column :as column]
   [zero-one.geni.core.data-sources :as data-sources]
   [zero-one.geni.core.dataset :as dataset]
   [zero-one.geni.core.dataset-creation :as dataset-creation]
   [zero-one.geni.core.polymorphic :as polymorphic]
   [zero-one.geni.core.functions :as sql]
   [zero-one.geni.core.window :as window])
  (:import
   (org.apache.spark.sql Column functions)))

;; NumPy
(defn clip
  "Returns a new Column where values outside `[low, high]` are clipped to the interval edges."
  [expr low high]
  (let [col (column/->column expr)]
    (-> (polymorphic/coalesce
         (sql/when (column/<= col low) low)
         (sql/when (column/<= high col) high)
         col)
        (polymorphic/as (format "clip(%s, %s, %s)"
                                (.toString col)
                                (str low)
                                (str high))))))

(defn random-uniform
  "Returns a new Column of draws from a uniform distribution."
  ([] (random-uniform 0.0 1.0))
  ([low high] (random-uniform low high (rand-int Integer/MAX_VALUE)))
  ([low high seed]
   (let [length (Math/abs (- high low))
         base   (min high low)]
     (column/+ base (column/* length (sql/rand seed))))))
(import-fn random-uniform runiform)
(import-fn random-uniform runif)

(defn random-norm
  "Returns a new Column of draws from a normal distribution."
  ([] (random-norm 0.0 1.0))
  ([mu sigma] (random-norm mu sigma (rand-int Integer/MAX_VALUE)))
  ([mu sigma seed] (column/+ mu (column/* sigma (sql/randn seed)))))
(import-fn random-norm rnorm)

(defn random-exp
  "Returns a new Column of draws from an exponential distribution."
  ([] (random-exp 1.0))
  ([rate] (random-exp rate (rand-int Integer/MAX_VALUE)))
  ([rate seed] (-> (sql/rand seed)
                   sql/log
                   (column/* -1.0)
                   (column// rate))))
(import-fn random-exp rexp)

(defn random-int
  "Returns a new Column of random integers from `low` (inclusive) to `high` (exclusive)."
  ([] (random-int 0 (dec Integer/MAX_VALUE)))
  ([low high] (random-int low high (rand-int Integer/MAX_VALUE)))
  ([low high seed]
   (let [length (Math/abs (- high low))
         base   (min high low)
         ->long #(column/cast % "long")]
     (column/+ (->long base) (->long (column/* length (sql/rand seed)))))))

(defn random-choice
  "Returns a new Column of a random sample from a given collection of `choices`."
  ([choices]
   (let [n-choices (count choices)]
     (random-choice choices (take n-choices (repeat (/ 1.0 n-choices))))))
  ([choices probs] (random-choice choices probs (rand-int Integer/MAX_VALUE)))
  ([choices probs seed]
   (assert (and (= (count choices) (count probs))
                (every? pos? probs))
           "random-choice args must have same lengths.")
   (assert (< (Math/abs (- (apply + probs) 1.0)) 1e-4)
           "random-choice probs must some to one.")
   (let [rand-col    (column/->column (sql/rand seed))
         cum-probs   (reductions + probs)
         choice-cols (map (fn [choice prob]
                            (sql/when (column/< rand-col (+ prob 1e-6))
                              (column/->column choice)))
                          choices
                          cum-probs)]
     (.as (apply polymorphic/coalesce choice-cols)
          (format "choice(%s, %s)" (str choices) (str probs))))))
(import-fn random-choice rchoice)

;; Pandas
(defn value-counts
  "Returns a Dataset containing counts of unique rows.

  The resulting object will be in descending order so that the
  first element is the most frequently-occurring element."
  [dataframe]
  (-> dataframe
      (dataset/group-by (dataset/columns dataframe))
      (dataset/agg {:count (functions/count "*")})
      (dataset/order-by (.desc (column/->column :count)))))

(defn shape
  "Returns a vector representing the dimensionality of the Dataset."
  [dataframe]
  [(.count dataframe) (count (.columns dataframe))])

(defn nlargest
  "Return the Dataset with the first `n-rows` rows ordered by `expr` in descending order."
  [dataframe n-rows expr]
  (-> dataframe
      (dataset/order-by (.desc (column/->column expr)))
      (dataset/limit n-rows)))

(defn nsmallest
  "Return the Dataset with the first `n-rows` rows ordered by `expr` in ascending order."
  [dataframe n-rows expr]
  (-> dataframe
      (dataset/order-by (column/->column expr))
      (dataset/limit n-rows)))

(defn nunique
  "Count distinct observations over all columns in the Dataset."
  [dataframe]
  (dataset/agg-all dataframe #(functions/countDistinct
                               (column/->column %)
                               (into-array Column []))))

(defn- resolve-probs [num-buckets-or-probs]
  (if (coll? num-buckets-or-probs)
    (do
      (assert (and (apply < num-buckets-or-probs)
                   (every? #(< 0.0 % 1.0) num-buckets-or-probs))
              "Probs array must be increasing and in the unit interval.")
      num-buckets-or-probs)
    (map #(/ (inc %) (double num-buckets-or-probs)) (range (dec num-buckets-or-probs)))))

(defn qcut
  "Returns a new Column of discretised `expr` into equal-sized buckets based
  on rank or based on sample quantiles."
  [expr num-buckets-or-probs]
  (let [probs     (resolve-probs num-buckets-or-probs)
        col       (column/->column expr)
        rank-col  (window/windowed {:window-col (sql/percent-rank) :order-by col})
        qcut-cols (map (fn [low high]
                         (sql/when (column/<= low rank-col high)
                           (column/lit (format "%s[%s, %s]"
                                               (.toString col)
                                               (str low)
                                               (str high)))))
                       (concat [0.0] probs)
                       (concat probs [1.0]))]
    (.as (apply polymorphic/coalesce qcut-cols)
         (format "qcut(%s, %s)" (.toString col) (str probs)))))

(defn cut
  "Returns a new Column of discretised `expr` into the intervals of bins."
  [expr bins]
  (assert (apply < bins))
  (let [col      (column/->column expr)
        cut-cols (map (fn [low high]
                        (sql/when (column/<= low col high)
                          (column/lit (format "%s[%s, %s]"
                                              (.toString col)
                                              (str low)
                                              (str high)))))
                      (concat [Double/NEGATIVE_INFINITY] bins)
                      (concat bins [Double/POSITIVE_INFINITY]))]
    (.as (apply polymorphic/coalesce cut-cols)
         (format "cut(%s, %s)" (.toString col) (str bins)))))

;; Tech ML
(defn- apply-options [dataset options]
  (-> dataset
      (cond-> (:column-whitelist options)
        (dataset/select (map name (:column-whitelist options))))
      (cond-> (:n-records options)
        (dataset/limit (:n-records options)))))

(defmulti ->dataset
  "Create a Dataset from a path or a collection of records."
  (fn [head & _] (class head)))

;; TODO: support excel files
(defmethod ->dataset java.lang.String
  ([path]
   (cond
     (string/includes? path ".avro") (data-sources/read-avro! path)
     (string/includes? path ".csv") (data-sources/read-csv! path)
     (string/includes? path ".json") (data-sources/read-json! path)
     (string/includes? path ".parquet") (data-sources/read-parquet! path)
     :else (throw (Exception. "Unsupported file format."))))
  ([path options] (apply-options (->dataset path) options)))

(defmethod ->dataset :default
  ([records] (dataset-creation/records->dataset records))
  ([records options] (apply-options (->dataset records) options)))

(import-fn dataset-creation/map->dataset name-value-seq->dataset)

(import-fn dataset/select select-columns)
