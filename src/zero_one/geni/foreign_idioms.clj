(ns zero-one.geni.foreign-idioms
  (:require
    [clojure.string :as string]
    [zero-one.geni.column :as column]
    [zero-one.geni.data-sources :as data-sources]
    [zero-one.geni.dataset :as dataset]
    [zero-one.geni.dataset-creation :as dataset-creation]
    [zero-one.geni.polymorphic :as polymorphic]
    [zero-one.geni.sql :as sql]
    [zero-one.geni.window :as window])
  (:import
    (org.apache.spark.sql Column functions)))

;; NumPy
(defn clip [expr low high]
  (let [col (column/->column expr)]
    (-> (polymorphic/coalesce
          (sql/when (column/<= col low) low)
          (sql/when (column/<= high col) high)
          col)
        (polymorphic/as (format "clip(%s, %s, %s)"
                                (.toString col)
                                (str low)
                                (str high))))))

(defn random-choice
  ([choices]
   (let [n-choices (count choices)]
     (random-choice choices (take n-choices (repeat (/ 1.0 n-choices))))))
  ([choices probs]
   (assert (and (= (count choices) (count probs))
                (every? pos? probs))
           "random-choice args must have same lengths.")
   (assert (< (Math/abs (- (apply + probs) 1.0)) 1e-4)
           "random-choice probs must some to one.")
   (let [rand-col    (column/->column (sql/rand))
         cum-probs   (reductions + probs)
         choice-cols (map (fn [choice prob]
                            (sql/when (column/< rand-col (+ prob 1e-6))
                              (column/->column choice)))
                          choices
                          cum-probs)]
     (.as (apply polymorphic/coalesce choice-cols)
          (format "choice(%s, %s)" (str choices) (str probs))))))

;; Pandas
(defn value-counts [dataframe]
  (-> dataframe
      (dataset/group-by (dataset/columns dataframe))
      (dataset/agg {:count (functions/count "*")})
      (dataset/order-by (.desc (column/->column :count)))))

(defn shape [dataframe]
  [(.count dataframe) (count (.columns dataframe))])

(defn nlargest [dataframe n-rows expr]
  (-> dataframe
      (dataset/order-by (.desc (column/->column expr)))
      (dataset/limit n-rows)))

(defn nsmallest [dataframe n-rows expr]
  (-> dataframe
      (dataset/order-by (column/->column expr))
      (dataset/limit n-rows)))

(defn nunique [dataframe]
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

(defn qcut [expr num-buckets-or-probs]
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

(defn cut [expr bins]
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
(defn apply-options [dataset options]
  (-> dataset
      (cond-> (:column-whitelist options)
        (dataset/select (map name (:column-whitelist options))))
      (cond-> (:n-records options)
        (dataset/limit (:n-records options)))))

(defmulti ->dataset (fn [head & _] (class head)))

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

(def name-value-seq->dataset dataset-creation/map->dataset)

(def select-columns dataset/select)
