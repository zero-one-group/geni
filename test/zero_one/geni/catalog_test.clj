(ns zero-one.geni.catalog-test
  (:require [clojure.test :refer [deftest is testing]]
            [zero-one.geni.catalog :as c]
            [zero-one.geni.core :as g]
            [zero-one.geni.test-resources :as tr]
            [clojure.string :as string])
  (:import (org.apache.spark.sql AnalysisException)
           (org.apache.spark.sql.catalog Catalog)
           (java.nio.file Paths)))

(defn create-test-db []
  (g/sql @tr/spark "CREATE DATABASE IF NOT EXISTS test_db"))

(deftest getting-the-default-catalog-test
  (is (instance? Catalog (c/catalog))))

(deftest cache-management-test
  (testing "Should (un)cache tables"
    (tr/with-fresh-session
      (let [df (g/range 1)]
        (create-test-db)
        (g/write-table! df "tbl")
        (g/write-table! df "test_db.tbl"))
      (c/cache-table "tbl")
      (c/cache-table @tr/spark "test_db.tbl")
      (is (c/cached? @tr/spark "tbl"))
      (is (c/cached? "test_db.tbl"))
      (c/uncache-table "tbl")
      (c/uncache-table "test_db.tbl")
      (is (not (c/cached? @tr/spark "tbl")))
      (is (not (c/cached? "test_db.tbl")))
      (c/cache-table "tbl")
      (c/cache-table "test_db.tbl")
      (c/clear-cache)
      (is (not (c/cached? "tbl")))
      (is (not (c/cached? "test_db.tbl"))))))

(deftest database-management-test
  (testing "Should change databases"
    (tr/with-fresh-session
      (create-test-db)
      (is (= "default" (c/current-database)))
      (c/set-current-database "test_db")
      (is (= "test_db" (c/current-database @tr/spark)))
      (c/set-current-database @tr/spark "default")
      (is (c/database-exists? "test_db"))
      (is (c/database-exists? @tr/spark "default"))))
  (testing "Should know what databases exist"
    (tr/with-fresh-session
      (is (= [{:name        "default"
               :catalog     "spark_catalog"
               :description "default database"}]
             (-> @tr/spark
                 c/list-databases
                 g/to-df
                 (g/drop :locationUri)
                 g/collect)))
      (create-test-db)
      (is (= [{:name        "default"
               :catalog     "spark_catalog"
               :description "default database"}
              {:name        "test_db"
               :catalog     "spark_catalog"
               :description ""}]
             (-> (c/list-databases)
                 g/to-df
                 (g/drop :locationUri)
                 g/collect))))))

(deftest table-management-test
  (testing "Know what tables exist"
    (tr/with-fresh-session
      (let [df1 (g/range 3)]
        (g/write-table! df1 "tbl1")
        (is (c/table-exists? "tbl1"))
        (is (= [{:name        "tbl1"
                 :catalog     "spark_catalog"
                 :namespace   ["default"]
                 :description nil
                 :tableType   "MANAGED"
                 :isTemporary false}]
               (-> (c/list-tables)
                   g/to-df
                   g/collect)))
        (c/drop-table "tbl1")
        (c/drop-table "i_dont_exist" true)
        (is (= []
               (-> (c/list-tables)
                   g/to-df
                   g/collect))))
      (testing "Know how to cache and un-cache tables"
        (let [df1 (g/range 3)]
          (is (thrown? AnalysisException (c/uncache-table @tr/spark "tbl1")))
          (g/write-table! df1 "tbl1")
          (c/cache-table "tbl1")
          (is (c/cached? "tbl1"))
          (c/uncache-table "tbl1")
          (is (not (c/cached? "tbl1")))
          (c/cache-table "tbl1" g/memory-only)
          (is (c/cached? "tbl1"))
          (c/clear-cache)
          (is (not (c/cached? "tbl1"))))))))

(deftest view-management-test
  (testing "Create and drop temp views"
    (tr/with-fresh-session
      (let [df (g/range 1)]
        (g/create-temp-view! df "view1")
        (g/create-global-temp-view! df "view2")
        (is (c/table-exists? "global_temp" "view2"))
        (is (= [{:name        "view2"
                 :catalog     nil
                 :namespace   ["global_temp"]
                 :description nil
                 :tableType   "TEMPORARY"
                 :isTemporary true}
                {:name        "view1"
                 :catalog     nil
                 :namespace   []
                 :description nil
                 :tableType   "TEMPORARY"
                 :isTemporary true}]
               (-> (c/list-tables "global_temp")
                   g/to-df
                   g/collect)))
        (c/drop-temp-view "view1")
        (c/drop-global-temp-view "view2")
        (is (not (c/table-exists? @tr/spark "default" "view1")))
        (is (not (c/table-exists? @tr/spark "view2"))))))
  (testing "Replace temp views"
    (tr/with-fresh-session
      (let [df1 (g/range 1)
            df2 (g/range 2)]
        (g/create-temp-view! df1 "view1")
        (g/create-global-temp-view! df1 "view2")
        (g/create-or-replace-temp-view! df2 "view1")
        (g/create-or-replace-global-temp-view! df2 "view2")
        (is (= 2 (g/count (g/read-table! "view1"))))
        (is (= 2 (g/count (g/read-table! "global_temp.view2"))))))))

(deftest exploring-columns-test

  (testing "list column in tables from any database"
    (let [df1 (g/range 3)
          df2 (g/with-column df1 :is_odd (g/mod :id 2))]
      (create-test-db)
      (g/write-table! df1 "tbl1")
      (g/write-table! df2 "tbl2")
      (g/write-table! df2 "test_db.tbl2")
      (is (= (concat (repeat 3 {:name        "id"
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
                                :isBucket    false}))
             ;; Spark 4 adds :isCluster.
             (map #(dissoc % :isCluster)
                  (-> (g/union (c/list-columns @tr/spark "tbl1")
                               (c/list-columns "tbl2")
                               (c/list-columns "test_db" "tbl2"))
                      (g/order-by :name)
                      g/to-df
                      g/collect)))))))

(deftest drop-test
  (testing "Drop tables"
    (tr/with-fresh-session
      (let [df (g/range 1)]
        (g/write-table! df "tbl")
        (c/drop-table "tbl")
        (is (not (c/table-exists? "tbl")))

        (g/write-table! df "tbl")
        (c/drop-table "default" "tbl")
        (is (not (c/table-exists? "default" "tbl")))

        (g/write-table! df "tbl")
        (c/drop-table @tr/spark "tbl")
        (is (not (c/table-exists? "tbl")))

        (g/write-table! df "tbl")
        (c/drop-table @tr/spark "default" "tbl")
        (is (not (c/table-exists? "default" "tbl")))

        (c/drop-table "tbl" true)
        (is (thrown? AnalysisException (c/drop-table "tbl")))

        (c/drop-table "default" "tbl" true)
        (is (thrown? AnalysisException (c/drop-table "default" "tbl")))

        (c/drop-table @tr/spark "tbl" true)
        (is (thrown? AnalysisException (c/drop-table @tr/spark "tbl")))

        (c/drop-table @tr/spark "default" "tbl" true)
        (is (thrown? AnalysisException (c/drop-table @tr/spark "default" "tbl"))))))
  (testing "Drop views"
    (tr/with-fresh-session
      (let [df (g/range 1)
            create-view #(g/sql @tr/spark (str "CREATE VIEW " % " AS SELECT * FROM tbl"))]
        (g/write-table! df "tbl")

        (create-view "v")
        (c/drop-view "v")
        (is (not (c/table-exists? "v")))

        (create-view "v")
        (c/drop-view "default" "v")
        (is (not (c/table-exists? "default" "v")))

        (create-view "v")
        (c/drop-view @tr/spark "v")
        (is (not (c/table-exists? "v")))

        (create-view "v")
        (c/drop-view @tr/spark "default" "v")
        (is (not (c/table-exists? "default" "v")))

        (c/drop-view "v" true)
        (is (thrown? AnalysisException (c/drop-view "v")))

        (c/drop-view "default" "v" true)
        (is (thrown? AnalysisException (c/drop-view "default" "v")))

        (c/drop-view @tr/spark "v" true)
        (is (thrown? AnalysisException (c/drop-view @tr/spark "v")))

        (c/drop-view @tr/spark "default" "v" true)
        (is (thrown? AnalysisException (c/drop-view @tr/spark "default" "v"))))))
  (testing "Drop relations"
    (tr/with-fresh-session
      (let [df (g/range 1)
            create-view #(g/sql @tr/spark (str "CREATE VIEW " % " AS SELECT * FROM tbl"))]
        (g/write-table! df "tbl")
        (create-view "v")

        (c/drop-relation :VIEW "v")
        (is (not (c/table-exists? "v")))

        (c/drop-relation :TABLE "default" "tbl")
        (is (not (c/table-exists? "default" "tbl")))

        (g/write-table! df "tbl")
        (create-view "v")

        (c/drop-relation @tr/spark :VIEW "v")
        (is (not (c/table-exists? "v")))

        (c/drop-relation @tr/spark :TABLE "default" "tbl")
        (is (not (c/table-exists? "default" "tbl")))

        (c/drop-relation :VIEW "v" true)
        (c/drop-relation :TABLE "tbl" true)
        (c/drop-relation :TABLE "default" "tbl" true)
        (is (thrown? AnalysisException (c/drop-relation :VIEW "v")))
        (is (thrown? AnalysisException (c/drop-relation :TABLE "tbl")))))))

(defn table-partition-path
  [spark table partition-col partition-val]
  (Paths/get (-> spark
                 .conf
                 (.get "spark.sql.warehouse.dir")
                 (string/replace "file:" ""))
             (into-array String [table (str (name partition-col) "=" partition-val)])))

(deftest refresh-test
  (testing "Adapt to change of table files"
    (tr/with-fresh-session
      (-> (g/range 6)
          (g/with-column :is_odd (g/mod :id 2))
          (g/write-table! "tbl" {:partition-by :is_odd}))
      (let [df (-> (g/read-table! "tbl") (g/order-by :id) (g/cache))]
        (tr/recursive-delete-dir (.toFile (table-partition-path @tr/spark "tbl" :is_odd 1)))
        ;(g/collect df) => (throws FileNotFoundException)  // @note I think will throw when not using local metastore.
        (c/refresh-table "tbl")
        (is (= [{:id 0 :is_odd 0} {:id 2 :is_odd 0} {:id 4 :is_odd 0}] (g/collect df))))))
  (testing "Adapt to change in data files"
    (tr/with-fresh-session
      (let [tmp-dir (tr/create-temp-dir!)
            path (str (.resolve (.toPath tmp-dir) "my_dataset"))]
        (-> (g/range 6)
            (g/with-column :is_odd (g/mod :id 2))
            (g/write-parquet! path {:partition-by :is_odd}))
        (let [df (-> (g/read-parquet! path) (g/order-by :id) (g/cache))
              partition-path (Paths/get path (into-array String ["is_odd=1"]))]
          (tr/recursive-delete-dir (.toFile partition-path))
          (c/refresh-by-path path)
          (is (= [{:id 0 :is_odd 0} {:id 2 :is_odd 0} {:id 4 :is_odd 0}] (g/collect df)))))))
  (testing "Recover partitions"
    (tr/with-fresh-session
      (-> (g/range 6)
          (g/with-column :is_odd (g/mod :id 2))
          (g/write-table! "tbl" {:partition-by :is_odd}))
      (let [df (-> (g/read-table! "tbl") (g/order-by :id) (g/cache))
            partition-path (table-partition-path @tr/spark "tbl" :is_odd 1)]
        (tr/recursive-delete-dir (.toFile partition-path))
        (c/recover-partitions "tbl")
        (is (= [{:id 0 :is_odd 0} {:id 2 :is_odd 0} {:id 4 :is_odd 0}] (g/collect df)))))))
