(ns zero-one.geni.catalog-test
  (:require [midje.sweet :refer [facts fact => throws with-state-changes after before]]
            [zero-one.geni.catalog :as c]
            [zero-one.geni.core :as g]
            [zero-one.geni.test-resources :as tr]
            [clojure.string :as string])
  (:import (org.apache.spark.sql AnalysisException)
           (org.apache.spark.sql.catalog Catalog)
           (java.nio.file Paths)))

(defn create-test-db []
  (g/sql @tr/spark "CREATE DATABASE IF NOT EXISTS test_db"))

(facts "On getting the default catalog"
  (instance? Catalog (c/catalog)) => true)

(facts "On cache management"
  (with-state-changes [(before :facts (tr/reset-session!))
                       (after :facts (tr/delete-warehouse!))]
    (fact "Should (un)cache tables"
      (let [df (g/range 1)]
        (create-test-db)
        (g/write-table! df "tbl")
        (g/write-table! df "test_db.tbl"))
      (c/cache-table "tbl")
      (c/cache-table @tr/spark "test_db.tbl")
      (c/cached? @tr/spark "tbl") => true
      (c/cached? "test_db.tbl") => true
      (c/uncache-table "tbl")
      (c/uncache-table "test_db.tbl")
      (c/cached? @tr/spark "tbl") => false
      (c/cached? "test_db.tbl") => false
      (c/cache-table "tbl")
      (c/cache-table "test_db.tbl")
      (c/clear-cache)
      (c/cached? "tbl") => false
      (c/cached? "test_db.tbl") => false)))

(facts "On database management"
  (with-state-changes [(before :facts (tr/reset-session!))
                       (after :facts (tr/delete-warehouse!))]
    (fact "Should change databases"
      (create-test-db)
      (c/current-database) => "default"
      (c/set-current-database "test_db")
      (c/current-database @tr/spark) => "test_db"
      (c/set-current-database @tr/spark "default")
      (c/database-exists? "test_db") => true
      (c/database-exists? @tr/spark "default") => true)
    (fact "Should know what databases exist"
      (-> @tr/spark
          c/list-databases
          g/to-df
          (g/drop :locationUri)
          g/collect)
      => [{:name        "default"
           :description "default database"}]
      (create-test-db)
      (-> (c/list-databases)
          g/to-df
          (g/drop :locationUri)
          g/collect)
      => [{:name        "default"
           :description "default database"}
          {:name        "test_db"
           :description ""}])))

(facts "On table management"
  (with-state-changes [(before :facts (tr/reset-session!))
                       (after :facts (tr/delete-warehouse!))]
    (fact "Should know what tables exist")
    (fact "Should know what columns exist")
    (fact "")))

(facts "On table management"
  (with-state-changes [(before :facts (tr/reset-session!))
                       (after :facts (tr/delete-warehouse!))]
    (fact "Know what tables exist"
      (let [df1 (g/range 3)]
        (g/write-table! df1 "tbl1")
        (c/table-exists? "tbl1") => true
        (-> (c/list-tables)
            g/to-df
            g/collect)
        => [{:name        "tbl1"
             :database    "default"
             :description nil
             :tableType   "MANAGED"
             :isTemporary false}]
        (c/drop-table "tbl1")
        (c/drop-table "i_dont_exist" true)
        (-> (c/list-tables)
            g/to-df
            g/collect)
        => [])
      (fact "Know how to cache and un-cache tables"
        (let [df1 (g/range 3)]
          (c/uncache-table @tr/spark "tbl1") => (throws AnalysisException)
          (g/write-table! df1 "tbl1")
          (c/cache-table "tbl1")
          (c/cached? "tbl1") => true
          (c/uncache-table "tbl1")
          (c/cached? "tbl1") => false
          (c/cache-table "tbl1" g/memory-only)
          (c/cached? "tbl1") => true
          (c/clear-cache)
          (c/cached? "tbl1") => false)))))

(facts "On view management"
  (with-state-changes [(before :facts (tr/reset-session!))
                       (after :facts (tr/delete-warehouse!))]
    (fact "Create and drop temp views"
      (let [df (g/range 1)]
        (g/create-temp-view! df "view1")
        (g/create-global-temp-view! df "view2")
        (c/table-exists? "global_temp" "view2") => true
        (-> (c/list-tables "global_temp")
            g/to-df
            g/collect)
        => [{:name        "view2"
             :database    "global_temp"
             :description nil
             :tableType   "TEMPORARY"
             :isTemporary true}
            {:name        "view1"
             :database    nil
             :description nil
             :tableType   "TEMPORARY"
             :isTemporary true}]
        (c/drop-temp-view "view1")
        (c/drop-global-temp-view "view2")
        (c/table-exists? @tr/spark "default" "view1") => false
        (c/table-exists? @tr/spark "view2") => false))
    (fact "Replace temp views"
      (let [df1 (g/range 1)
            df2 (g/range 2)]
        (g/create-temp-view! df1 "view1")
        (g/create-global-temp-view! df1 "view2")
        (g/create-or-replace-temp-view! df2 "view1")
        (g/create-or-replace-global-temp-view! df2 "view2")
        (g/count (g/read-table! "view1")) => 2
        (g/count (g/read-table! "global_temp.view2")) => 2))))

(facts "On exploring columns"
  (with-state-changes [(before :facts (tr/reset-session!))
                       (after :facts (tr/delete-warehouse!))])
  (fact "list column in tables from any database"
    (let [df1 (g/range 3)
          df2 (g/with-column df1 :is_odd (g/mod :id 2))]
      (create-test-db)
      (g/write-table! df1 "tbl1")
      (g/write-table! df2 "tbl2")
      (g/write-table! df2 "test_db.tbl2")
      (-> (g/union (c/list-columns @tr/spark "tbl1")
                   (c/list-columns "tbl2")
                   (c/list-columns "test_db" "tbl2"))
          (g/order-by :name)
          g/to-df
          g/collect)
      => (concat (repeat 3 {:name        "id"
                            :description nil
                            :dataType    "bigint"
                            :nullable    true
                            :isPartition false
                            :isBucket    false})
                 (repeat 2 {:name        "is_odd"
                            :description nil
                            :dataType    "bigint"
                            :nullable    true
                            :isPartition false
                            :isBucket    false})))))

(facts "On drop"
  (with-state-changes [(before :facts (tr/reset-session!))
                       (after :facts (tr/delete-warehouse!))]
    (fact "Drop tables"
      (let [df (g/range 1)]
        (g/write-table! df "tbl")
        (c/drop-table "tbl")
        (c/table-exists? "tbl") => false

        (g/write-table! df "tbl")
        (c/drop-table "default" "tbl")
        (c/table-exists? "default" "tbl") => false

        (g/write-table! df "tbl")
        (c/drop-table @tr/spark "tbl")
        (c/table-exists? "tbl") => false

        (g/write-table! df "tbl")
        (c/drop-table @tr/spark "default" "tbl")
        (c/table-exists? "default" "tbl") => false

        (c/drop-table "tbl" true)
        (c/drop-table "tbl") => (throws AnalysisException)

        (c/drop-table "default" "tbl" true)
        (c/drop-table "default" "tbl") => (throws AnalysisException)

        (c/drop-table @tr/spark "tbl" true)
        (c/drop-table @tr/spark "tbl") => (throws AnalysisException)

        (c/drop-table @tr/spark "default" "tbl" true)
        (c/drop-table @tr/spark "default" "tbl") => (throws AnalysisException)))
    (fact "Drop views"
      (let [df (g/range 1)
            create-view #(g/sql @tr/spark (str "CREATE VIEW " % " AS SELECT * FROM tbl"))]
        (g/write-table! df "tbl")

        (create-view "v")
        (c/drop-view "v")
        (c/table-exists? "v") => false

        (create-view "v")
        (c/drop-view "default" "v")
        (c/table-exists? "default" "v") => false

        (create-view "v")
        (c/drop-view @tr/spark "v")
        (c/table-exists? "v") => false

        (create-view "v")
        (c/drop-view @tr/spark "default" "v")
        (c/table-exists? "default" "v") => false

        (c/drop-view "v" true)
        (c/drop-view "v") => (throws AnalysisException)

        (c/drop-view "default" "v" true)
        (c/drop-view "default" "v") => (throws AnalysisException)

        (c/drop-view @tr/spark "v" true)
        (c/drop-view @tr/spark "v") => (throws AnalysisException)

        (c/drop-view @tr/spark "default" "v" true)
        (c/drop-view @tr/spark "default" "v") => (throws AnalysisException)))
    (fact "Drop relations"
      (let [df (g/range 1)
            create-view #(g/sql @tr/spark (str "CREATE VIEW " % " AS SELECT * FROM tbl"))]
        (g/write-table! df "tbl")
        (create-view "v")

        (c/drop-relation :VIEW "v")
        (c/table-exists? "v") => false

        (c/drop-relation :TABLE "default" "tbl")
        (c/table-exists? "default" "tbl") => false

        (g/write-table! df "tbl")
        (create-view "v")

        (c/drop-relation @tr/spark :VIEW "v")
        (c/table-exists? "v") => false

        (c/drop-relation @tr/spark :TABLE "default" "tbl")
        (c/table-exists? "default" "tbl") => false

        (c/drop-relation :VIEW "v" true)
        (c/drop-relation :TABLE "tbl" true)
        (c/drop-relation :TABLE "default" "tbl" true)
        (c/drop-relation :VIEW "v") => (throws AnalysisException)
        (c/drop-relation :TABLE "tbl") => (throws AnalysisException)))))

(defn table-partition-path
  [spark table partition-col partition-val]
  (Paths/get (-> spark
                 .conf
                 (.get "spark.sql.warehouse.dir")
                 (string/replace "file:" ""))
             (into-array String [table (str (name partition-col) "=" partition-val)])))

(facts "On refresh"
  (with-state-changes [(before :facts (tr/reset-session!))
                       (after :facts (tr/delete-warehouse!))]
    (fact "Adapt to change of table files"
      (-> (g/range 6)
          (g/with-column :is_odd (g/mod :id 2))
          (g/write-table! "tbl" {:partition-by :is_odd}))
      (let [df (-> (g/read-table! "tbl") (g/order-by :id) (g/cache))]
        (tr/recursive-delete-dir (.toFile (table-partition-path @tr/spark "tbl" :is_odd 1)))
        ;(g/collect df) => (throws FileNotFoundException)  // @note I think will throw when not using local metastore.
        (c/refresh-table "tbl")
        (g/collect df) => [{:id 0 :is_odd 0} {:id 2 :is_odd 0} {:id 4 :is_odd 0}]))
    (fact "Adapt to change in data files"
      (let [tmp-dir (tr/create-temp-dir!)
            path (str (.resolve (.toPath tmp-dir) "my_dataset"))]
        (-> (g/range 6)
            (g/with-column :is_odd (g/mod :id 2))
            (g/write-parquet! path {:partition-by :is_odd}))
        (let [df (-> (g/read-parquet! path) (g/order-by :id) (g/cache))
              partition-path (Paths/get path (into-array String ["is_odd=1"]))]
          (tr/recursive-delete-dir (.toFile partition-path))
          (c/refresh-by-path path)
          (g/collect df) => [{:id 0 :is_odd 0} {:id 2 :is_odd 0} {:id 4 :is_odd 0}])))
    (fact "Recover partitions"
      (-> (g/range 6)
          (g/with-column :is_odd (g/mod :id 2))
          (g/write-table! "tbl" {:partition-by :is_odd}))
      (let [df (-> (g/read-table! "tbl") (g/order-by :id) (g/cache))
            partition-path (table-partition-path @tr/spark "tbl" :is_odd 1)]
        (tr/recursive-delete-dir (.toFile partition-path))
        (c/recover-partitions "tbl")
        (g/collect df) => [{:id 0 :is_odd 0} {:id 2 :is_odd 0} {:id 4 :is_odd 0}]))))
