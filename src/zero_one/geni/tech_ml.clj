(ns zero-one.geni.tech-ml
  (:require
    [clojure.string :as string]
    [zero-one.geni.dataset :as dataset]
    [zero-one.geni.data-sources :as data-sources]
    [zero-one.geni.dataset-creation :as dataset-creation]))

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
