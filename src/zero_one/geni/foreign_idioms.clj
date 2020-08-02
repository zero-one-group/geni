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

(defn random-uniform
  ([] (random-uniform 0.0 1.0))
  ([low high] (random-uniform low high (rand-int Integer/MAX_VALUE)))
  ([low high seed]
   (let [length (Math/abs (- high low))
         base   (min high low)]
     (column/+ base (column/* length (sql/rand seed))))))
(def runiform random-uniform)
(def runif random-uniform)

(defn random-norm
  ([] (random-norm 0.0 1.0))
  ([mu sigma] (random-norm mu sigma (rand-int Integer/MAX_VALUE)))
  ([mu sigma seed] (column/+ mu (column/* sigma (sql/randn seed)))))
(def rnorm random-norm)

(defn random-exp
  ([] (random-exp 1.0))
  ([rate] (random-exp rate (rand-int Integer/MAX_VALUE)))
  ([rate seed] (-> (sql/rand seed)
                   sql/log
                   (column/* -1.0)
                   (column// rate))))
(def rexp random-exp)

(defn random-int
  ([] (random-int 0 (dec Integer/MAX_VALUE)))
  ([low high] (random-int low high (rand-int Integer/MAX_VALUE)))
  ([low high seed]
   (column/cast (column/- (random-uniform low high seed) 1) "long")))

(defn random-choice
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
(def rchoice random-choice)

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
