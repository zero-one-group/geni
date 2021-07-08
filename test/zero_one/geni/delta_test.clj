(ns zero-one.geni.delta-test
  (:require [midje.sweet :refer [facts fact => throws with-state-changes after before]]
            [zero-one.geni.delta :as d]
            [zero-one.geni.core :as g]
            [zero-one.geni.test-resources :as tr])
  (:import (java.io File)))

(defn stable-collect [ds]
  (-> ds g/to-df (g/order-by :id) g/collect))

(facts "On Delta table creation"
  (with-state-changes [(before :facts (tr/reset-session!))
                       (after :facts (tr/delete-warehouse!))]
    (fact "Can read and write paths"
      (let [data (g/range 3)
            table-path (tr/join-paths (.toString (tr/create-temp-dir!)) "delta_test")]
        (g/write-delta! data table-path)
        (let [delta-tbl (d/delta-table {:location table-path})]
          (d/delta-table? table-path) => true
          (g/collect (g/order-by (d/to-df delta-tbl) :id)) => (g/collect (g/order-by (g/to-df data) :id)))))
    (fact "Can read and write managed tables"
      (let [data (g/range 3)]
        (g/write-table! data "test_tbl")
        (d/convert-to-delta "test_tbl")
        (let [delta-tbl (d/delta-table {:location "test_tbl"
                                        :format   :table})]
          (g/collect (g/order-by (d/to-df delta-tbl) :id)) => (g/collect (g/order-by (g/to-df data) :id)))))))

(facts "On simple Delta operations"
  (let [data (g/range 5)
        table-path (tr/join-paths (.toString (tr/create-temp-dir!)) "delta_test")]
    (g/write-delta! data table-path)
    (let [delta-tbl (d/delta-table {:location table-path})]
      (fact "Delete rows by condition"
        (d/delete delta-tbl (g/>= (g/col :id) (g/lit 3)))
        (stable-collect (d/to-df delta-tbl)) => (stable-collect (g/range 3)))

      ;; @todo Uncomment these tests once Delta has been upgraded to support Spark 3.1.x
      ;; These tests pass against Spark 3.0.2
      ;(fact "Update all rows"
      ;  (d/update delta-tbl {:id (g/* (g/col :id) 2)})
      ;  (stable-collect (d/to-df delta-tbl)) => [{:id 0} {:id 2} {:id 4}])
      ;
      ;(fact "Update rows by condition"
      ;  (d/update delta-tbl
      ;            (g/=== (g/col :id) (g/lit 0))
      ;            {:id (g/lit 10)})
      ;  (stable-collect (d/to-df delta-tbl)) => [{:id 2} {:id 4} {:id 10}])

      (fact "Delete all rows"
        (d/delete delta-tbl)
        (stable-collect (d/to-df delta-tbl)) => [])

      (fact "Atomic (transactional) appends"
        (g/write-delta! (g/range 3) table-path {:mode "append"}))

      (fact "Read version history"
        (-> (d/history delta-tbl)
            (g/order-by :version)
            (g/collect-col :operation)) => ["WRITE" "DELETE" "DELETE" "WRITE"]) ;; Should be ["WRITE" "DELETE" "UPDATE" "UPDATE" "DELETE" "WRITE"]

      (fact "Vacuuming files that are no longer active"
        (g/with-settings
          @tr/spark
          {:spark.databricks.delta.retentionDurationCheck.enabled false}
          (fn [_]
            (d/vacuum delta-tbl 0.0)))

        ;; Convert to normal parquet (requires vacuuming first)
        ;; https://docs.delta.io/0.8.0/delta-utility.html#convert-a-delta-table-to-a-parquet-table
        (-> (tr/join-paths table-path "_delta_log")
            File.
            tr/recursive-delete-dir)
        (stable-collect (g/read-parquet! table-path)) => (stable-collect (g/range 3))))))

;; @todo Uncomment these tests once Delta has been upgraded to support Spark 3.1.x
;; These tests pass against Spark 3.0.2
;(facts "On Delta merge"
;  (let [data (g/records->dataset [{:id 0 :s "A" :b false}
;                                  {:id 1 :s "B" :b false}
;                                  {:id 2 :s "C" :b false}])]
;    (fact "Delete when matched; insert when not matched"
;      (let [table-path (tr/join-paths (.toString (tr/create-temp-dir!)) "delta_test")
;            _ (g/write-delta! data table-path)
;            delta-tbl (d/delta-table {:location table-path})]
;        (d/merge (d/as delta-tbl "dest")
;                 (-> [{:id 1 :s "Z" :b true}
;                      {:id 3 :s "Z" :b true}]
;                     g/records->dataset
;                     (g/as "src"))
;                 {::d/condition        (g/=== (g/col :dest.id) (g/col :src.id))
;                  ::d/when-matched     [{::d/on-match :delete ::d/column-rule :all}]
;                  ::d/when-not-matched {::d/on-not-match :insert ::d/column-rule :all}})
;        (stable-collect (d/to-df delta-tbl)) => [{:id 0 :s "A" :b false}
;                                                 {:id 2 :s "C" :b false}
;                                                 {:id 3 :s "Z" :b true}]
;        (tr/recursive-delete-dir (File. table-path))))
;    (fact "Update all when matched; insert when not matched"
;      (let [table-path (tr/join-paths (.toString (tr/create-temp-dir!)) "delta_test")
;            _ (g/write-delta! data table-path)
;            delta-tbl (d/delta-table {:location table-path})]
;        (d/merge (d/as delta-tbl "dest")
;                 (-> [{:id 1 :s "Z" :b true}
;                      {:id 3 :s "Z" :b true}]
;                     g/records->dataset
;                     (g/as "src"))
;                 {::d/condition        (g/=== (g/col :dest.id) (g/col :src.id))
;                  ::d/when-matched     [{::d/on-match :update ::d/column-rule :all}]
;                  ::d/when-not-matched {::d/on-not-match :insert ::d/column-rule :all}})
;        (stable-collect (d/to-df delta-tbl)) => [{:id 0 :s "A" :b false}
;                                                 {:id 1 :s "Z" :b true}
;                                                 {:id 2 :s "C" :b false}
;                                                 {:id 3 :s "Z" :b true}]
;        (tr/recursive-delete-dir (File. table-path))))
;    (fact "Update on condition when matched; noop when not matched"
;      (let [table-path (tr/join-paths (.toString (tr/create-temp-dir!)) "delta_test")
;            _ (g/write-delta! data table-path)
;            delta-tbl (d/delta-table {:location table-path})]
;        (g/write-delta! data table-path {:mode "overwrite"})
;        (d/merge (d/as delta-tbl "dest")
;                 (-> [{:id 1 :s "Z" :b true}
;                      {:id 2 :s "Z" :b false}
;                      {:id 3 :s "Z" :b true}]
;                     g/records->dataset
;                     (g/as "src"))
;                 {::d/condition    (g/=== (g/col :dest.id) (g/col :src.id))
;                  ::d/when-matched [{::d/condition   (g/col :src.b)
;                                     ::d/on-match    :update
;                                     ::d/column-rule {:dest.id (g/* (g/col :dest.id) (g/lit 10))}}]})
;        (stable-collect (d/to-df delta-tbl)) => [{:id 0 :s "A" :b false}
;                                                 {:id 2 :s "C" :b false}
;                                                 {:id 10 :s "B" :b false}]))
;    (fact "Noop when matched; insert on condition when not matched"
;      (let [table-path (tr/join-paths (.toString (tr/create-temp-dir!)) "delta_test")
;            _ (g/write-delta! data table-path)
;            delta-tbl (d/delta-table {:location table-path})]
;        (g/write-delta! data table-path {:mode "overwrite"})
;        (d/merge (d/as delta-tbl "dest")
;                 (-> [{:id 1 :s "Z" :b true}
;                      {:id 3 :s "Z" :b false}
;                      {:id 4 :s "Z" :b true}]
;                     g/records->dataset
;                     (g/as "src"))
;                 {::d/condition        (g/=== (g/col :dest.id) (g/col :src.id))
;                  ::d/when-not-matched {::d/condition    (g/col :src.b)
;                                        ::d/on-not-match :insert
;                                        ::d/column-rule  :all}})
;        (stable-collect (d/to-df delta-tbl)) => [{:id 0 :s "A" :b false}
;                                                 {:id 1 :s "B" :b false}
;                                                 {:id 2 :s "C" :b false}
;                                                 {:id 4 :s "Z" :b true}]))
;    (fact "Update on condition when matched; update all when matched; noop when not matched."
;      (let [table-path (tr/join-paths (.toString (tr/create-temp-dir!)) "delta_test")
;            _ (g/write-delta! data table-path)
;            delta-tbl (d/delta-table {:location table-path})]
;        (g/write-delta! data table-path {:mode "overwrite"})
;        (d/merge (d/as delta-tbl "dest")
;                 (-> [{:id 0 :s "Z" :b false}
;                      {:id 1 :s "Z" :b true}
;                      {:id 2 :s "Z" :b false}]
;                     g/records->dataset
;                     (g/as "src"))
;                 {::d/condition        (g/=== (g/col :dest.id) (g/col :src.id))
;                  ::d/when-matched     [{::d/condition   (g/col :src.b)
;                                         ::d/on-match    :update
;                                         ::d/column-rule {:dest.id (g/* (g/col :dest.id) (g/lit 10))}}
;                                        {::d/on-match :update
;                                         ::d/column-rule :all}]})
;        (stable-collect (d/to-df delta-tbl)) => [{:id 0 :s "Z" :b false}
;                                                 {:id 2 :s "Z" :b false}
;                                                 {:id 10 :s "B" :b false}]))
;    (fact "Noop when matched; insert with column rule when not matched"
;      (let [table-path (tr/join-paths (.toString (tr/create-temp-dir!)) "delta_test")
;            _ (g/write-delta! data table-path)
;            delta-tbl (d/delta-table {:location table-path})]
;        (g/write-delta! data table-path {:mode "overwrite"})
;        (d/merge (d/as delta-tbl "dest")
;                 (-> [{:id 3 :s "Z" :b false}
;                      {:id 4 :s "Z" :b true}]
;                     g/records->dataset
;                     (g/as "src"))
;                 {::d/condition        (g/=== (g/col :dest.id) (g/col :src.id))
;                  ::d/when-not-matched {::d/condition    (g/col :src.b)
;                                        ::d/on-not-match :insert
;                                        ::d/column-rule  {:dest.id (g/* (g/col :src.id) (g/lit 10))}}})
;        (stable-collect (d/to-df delta-tbl)) => [{:id 0 :s "A" :b false}
;                                                 {:id 1 :s "B" :b false}
;                                                 {:id 2 :s "C" :b false}
;                                                 {:id 40 :s nil :b nil}]))))
