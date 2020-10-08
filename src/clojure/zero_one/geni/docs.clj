(ns zero-one.geni.docs
  (:require
    [clojure.java.io :as io]
    [taoensso.nippy :as nippy])
  (:import
    (org.apache.commons.io IOUtils)))

(def spark-docs
  (-> "spark-docs.nippy"
      io/resource
      io/input-stream
      IOUtils/toByteArray
      nippy/thaw))

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
