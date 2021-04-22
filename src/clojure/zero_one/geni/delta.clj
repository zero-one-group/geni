(ns zero-one.geni.delta
  (:refer-clojure :exclude [update merge])
  (:require [clojure.spec.alpha :as s]
            [zero-one.geni.core :as g]
            [zero-one.geni.defaults :as defaults]
            [zero-one.geni.interop :as i]
            [zero-one.geni.utils :refer [with-dynamic-import]])
  (:import (org.apache.spark.sql Column Dataset SparkSession)
           (java.util Map)))

(s/def ::format #{:path :table})
(s/def ::column-name (s/or :kw keyword? :str string?))
(s/def ::column #(instance? Column %))
(s/def ::condition ::column)
(s/def ::on-match #{:delete :update})
(s/def ::on-not-match #{:insert})

(s/def ::column-rule
  (s/or :some (s/map-of ::column-name ::column)
        :all #{:all}))

(s/def ::when-matched-clause
  (s/keys :req [::on-match ::column-rule]
          :opt [::condition]))

(s/def ::when-matched
  (s/coll-of ::when-matched-clause :kind sequential? :max-count 2))

(s/def ::when-not-matched
  (s/keys :req [::on-not-match ::column-rule]
          :opt [::condition]))

(s/def ::merge-strategy
  (s/keys :req [::condition]
          :opt [::when-matched ::when-not-matched]))

;; @todo Uncomment this code once Delta has been upgraded to support Spark 3.1.x
;; This code has been tested against Spark 3.0.2
;(defn validate-merge-strategy
;  "
;  See constraints: https://docs.delta.io/latest/api/scala/io/delta/tables/DeltaMergeBuilder.html"
;  [merge-strategy]
;  {:pre [(s/valid? ::merge-strategy merge-strategy)]}
;  (let [when-matched (get merge-strategy ::when-matched [])]
;    ; There can be at most one `update` action and one `delete` action in when-matched clauses.
;    (let [clause-counts (->> when-matched
;                             (group-by ::on-match)
;                             (map (fn [[k v]] [k (count v)])))]
;      (when (> (count clause-counts) 2)
;        (ex-info (str "Found more than 2 `when-matched` clauses in a Delta merge strategy. There can only be 1 update and 1 delete.")
;                 {:clause-kinds    (keys clause-counts)
;                  ::merge-strategy merge-strategy}))
;      (doseq [[clause-kind clause-count] clause-counts]
;        (when (> clause-count 1)
;          (ex-info (str "Found multiple " clause-kind " clauses in a Delta merge strategy.")
;                   {:clause-kind     clause-kind
;                    :clause-count    clause-count
;                    ::merge-strategy merge-strategy}))))
;
;    ; If there are two when-matched clauses, then the first one must have a condition.
;    (when (and (= (count when-matched) 2)
;               (= (::column-rule (first when-matched)) :all))
;      (ex-info "When using two when-matched clauses in a Delta merge the first one must have a condition, not :all."
;               {::merge-strategy merge-strategy})))
;  true)
;
;(defn- prepare-update-set-map ^Map
;  [m]
;  (->> m
;       (map (fn [[k v]] [(name k) (g/col v)]))
;       (into {})))

(with-dynamic-import
  [[io.delta.tables DeltaTable DeltaMergeBuilder]]

  (defn delta-table ^DeltaTable
    ([opts] (delta-table @defaults/spark opts))
    ([^SparkSession spark {:keys [format location] :or {format :path}}]
     {:pre [(s/valid? ::format format)]}
     (if (= format :path)
       (. DeltaTable forPath spark location)
       (. DeltaTable forName spark location))))

  (defn delta-table?
    ([^String identifier]
     (DeltaTable/isDeltaTable identifier))
    ([^SparkSession spark ^String identifier]
     (DeltaTable/isDeltaTable spark identifier)))

  (defn as ^DeltaTable
    [^DeltaTable table ^String alias]
    ;; @todo move to polymorphic.clj? How to handle dynamic import in multimethod?
    (.as table alias))

  (defn to-df ^Dataset
    [^DeltaTable table]
    ;; @todo move to polymorphic.clj? How to handle dynamic import in multimethod?
    (.toDF table))

  (defmulti convert-to-delta (fn [head & _] (class head)))
  (defmethod convert-to-delta :default
    [^String identifier & {:keys [partition-schema]}]
    (convert-to-delta @defaults/spark identifier :partition-schema partition-schema))
  (defmethod convert-to-delta SparkSession
    [^SparkSession spark ^String identifier & {:keys [partition-schema]}]
    (if (nil? partition-schema)
      (DeltaTable/convertToDelta spark identifier)
      (DeltaTable/convertToDelta spark identifier (g/->schema partition-schema))))

  (defmulti delete (fn [& args] (class (last args))))
  (defmethod delete :default
    [^DeltaTable table]
    (.delete table))
  (defmethod delete Column
    [^DeltaTable table ^Column condition]
    (.delete table condition))

  (defn history ^Dataset
    ([^DeltaTable table]
     (.history table))
    ([^DeltaTable table ^Integer limit]
     (.history table limit)))

  (defn vacuum
    ([^DeltaTable table]
     (.vacuum table))
    ([^DeltaTable table ^Double retention-hours]
     (.vacuum table retention-hours)))

  ;; @todo Uncomment this code once Delta has been upgraded to support Spark 3.1.x
  ;; This code has been tested against Spark 3.0.2
  ;(defn update
  ;  ;; @todo Update is broken with Spark 3.1 + Delta 0.8. upgrade Delta ASAP.
  ;  ;; https://github.com/delta-io/delta/issues/594
  ;  ([^DeltaTable table set-to]
  ;   (let [m (prepare-update-set-map set-to)]
  ;     (.update table m)))
  ;  ([^DeltaTable table ^Column condition set-to]
  ;   (.update table condition (prepare-update-set-map set-to))))
  ;
  ;(defn- apply-when-matched-clause ^DeltaMergeBuilder
  ;  [^DeltaMergeBuilder merge-builder when-matched-clause]
  ;  (let [{:zero-one.geni.delta/keys [condition on-match column-rule]} when-matched-clause
  ;        on-match-builder (if (nil? condition)
  ;                           (.whenMatched merge-builder)
  ;                           (.whenMatched merge-builder condition))]
  ;    (cond
  ;      (and (= on-match :update) (= column-rule :all))
  ;      (.updateAll on-match-builder)
  ;
  ;      (= on-match :update)
  ;      (let [scala-column-rule (->> column-rule
  ;                                   (map (fn [[k column]] [(name k) column]))
  ;                                   (into {})
  ;                                   (i/->scala-map))]
  ;        (.update on-match-builder scala-column-rule))
  ;
  ;      (and (= on-match :delete) (= column-rule :all))
  ;      (.delete on-match-builder)
  ;
  ;      (= on-match :delete)
  ;      (ex-info "If a Delta merge `when-matched` clause is set to `delete`, it must use the column-rule `all`."
  ;               {::when-matched-clause when-matched-clause})
  ;
  ;      :else
  ;      (ex-info (str "Unknown `on-match` for Delta merge strategy.")
  ;               {::when-matched-clause when-matched-clause}))))
  ;
  ;(defn- apply-when-not-matched ^DeltaMergeBuilder
  ;  [^DeltaMergeBuilder merge-builder when-not-matched]
  ;  (let [{:zero-one.geni.delta/keys [on-not-match column-rule condition]} when-not-matched
  ;        not-match-builder (if (nil? condition)
  ;                            (.whenNotMatched merge-builder)
  ;                            (.whenNotMatched merge-builder condition))]
  ;    (cond
  ;      (and (= on-not-match :insert) (= column-rule :all))
  ;      (.insertAll not-match-builder)
  ;
  ;      (= on-not-match :insert)
  ;      (let [scala-column-rule (->> column-rule
  ;                                   (map (fn [[k column]] [(name k) column]))
  ;                                   (into {})
  ;                                   (i/->scala-map))]
  ;        (.insert not-match-builder scala-column-rule))
  ;
  ;      :else
  ;      (ex-info (str "Unknown `on-not-match` for Delta merge strategy.")
  ;               {::when-not-matched when-not-matched}))))
  ;
  ;(defn merge
  ;  [^DeltaTable destination ^Dataset source merge-strategy]
  ;  {:pre [(validate-merge-strategy merge-strategy)]}
  ;  ;; @todo Update is broken with Spark 3.1 + Delta 0.8. upgrade Delta ASAP.
  ;  ;; https://github.com/delta-io/delta/issues/594
  ;  (let [merge-builder (.merge destination source (::condition merge-strategy))
  ;        with-on-matched (reduce (fn [builder clause]
  ;                                  (apply-when-matched-clause builder clause))
  ;                                merge-builder
  ;                                (get merge-strategy ::when-matched []))
  ;        with-not-matched (let [clause (::when-not-matched merge-strategy)]
  ;                           (if (nil? clause)
  ;                             with-on-matched
  ;                             (apply-when-not-matched with-on-matched clause)))]
  ;    (.execute with-not-matched)))
  )
