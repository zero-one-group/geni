(ns zero-one.geni.docs
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io])
  (:import
   (java.io PushbackReader)))

(def spark-docs
  "Docstrings scraped from Spark's Scaladoc, see scripts/scrape-spark-docs.clj."
  (with-open [reader (-> "spark-docs.edn" io/resource io/reader PushbackReader.)]
    (edn/read reader)))

(defn no-doc? [v]
  (-> v meta :doc nil?))

(defn add-doc! [fn-var doc]
  (alter-meta! fn-var assoc :doc doc))

(defn alter-doc! [fn-name fn-var doc-maps]
  (let [fn-key (keyword fn-name)
        doc    (->> doc-maps (mapv fn-key) (remove nil?) first)]
    (when (and doc (no-doc? fn-var))
      (add-doc! fn-var doc))))

(defn alter-docs-in-ns! [ns-sym doc-maps]
  (let [public-vars (ns-publics ns-sym)]
    (mapv
     (fn [[fn-name fn-var]]
       (alter-doc! fn-name fn-var doc-maps))
     public-vars)
    :succeeded))

(defn docless-vars [ns-sym]
  (->> (ns-publics ns-sym)
       (mapv second)
       (filter no-doc?)))

(defn invalid-doc-vars [ns-sym]
  (->> (ns-publics ns-sym)
       (filter (fn [[_ v]]
                 (let [doc (-> v meta :doc)]
                   (not (or (nil? doc) (string? doc))))))
       (into {})))
