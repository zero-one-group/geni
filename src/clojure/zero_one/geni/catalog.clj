(ns zero-one.geni.catalog
  (:require [zero-one.geni.defaults :as defaults])
  (:import (org.apache.spark.sql.catalog Catalog)
           (org.apache.spark.sql Dataset SparkSession)
           (org.apache.spark.storage StorageLevel)
           (clojure.lang Keyword)))


(defn table-identifier
  [database name] (str database "." name))


(defn catalog ^Catalog
  ([]
   (catalog @defaults/spark))
  ([^SparkSession spark]
   (. spark catalog)))


(defn- catalog-dispatch
  [& args]
  (mapv class args))


(defmulti cache-table catalog-dispatch)
(defmethod cache-table [String]
  [^String table-name]
  (cache-table @defaults/spark table-name))
(defmethod cache-table [String StorageLevel]
  [^String table-name ^StorageLevel storage-level]
  (cache-table @defaults/spark table-name storage-level))
(defmethod cache-table [SparkSession String]
  [^SparkSession spark ^String table-name]
  (cache-table (catalog spark) table-name))
(defmethod cache-table [SparkSession String StorageLevel]
  [^SparkSession spark ^String table-name ^StorageLevel storage-level]
  (cache-table (catalog spark) table-name storage-level))
(defmethod cache-table [Catalog String]
  [^Catalog catalog ^String table-name]
  (. catalog cacheTable table-name))
(defmethod cache-table [Catalog String StorageLevel]
  [^Catalog catalog ^String table-name ^StorageLevel storage-level]
  (. catalog cacheTable table-name storage-level))


(defmulti clear-cache catalog-dispatch)
(defmethod clear-cache []
  []
  (clear-cache @defaults/spark))
(defmethod clear-cache [SparkSession]
  [^SparkSession spark]
  (clear-cache (catalog spark)))
(defmethod clear-cache [Catalog]
  [^Catalog catalog]
  (. catalog clearCache))


(defmulti current-database ^String catalog-dispatch)
(defmethod current-database []
  []
  (current-database @defaults/spark))
(defmethod current-database [SparkSession]
  [^SparkSession spark]
  (current-database (catalog spark)))
(defmethod current-database [Catalog]
  [^Catalog catalog]
  (. catalog currentDatabase))


(defmulti database-exists? ^Boolean catalog-dispatch)
(defmethod database-exists? [String]
  [^String db-name]
  (database-exists? @defaults/spark db-name))
(defmethod database-exists? [SparkSession String]
  [^SparkSession spark ^String db-name]
  (database-exists? (catalog spark) db-name))
(defmethod database-exists? [Catalog String]
  [^Catalog catalog ^String db-name]
  (. catalog databaseExists db-name))


(defmulti drop-temp-view ^Boolean catalog-dispatch)
(defmethod drop-temp-view [String]
  [^String view-name]
  (drop-temp-view @defaults/spark view-name))
(defmethod drop-temp-view [SparkSession String]
  [^SparkSession spark ^String view-name]
  (drop-temp-view (catalog spark) view-name))
(defmethod drop-temp-view [Catalog String]
  [^Catalog catalog ^String view-name]
  (. catalog dropTempView view-name))


(defmulti drop-global-temp-view ^Boolean catalog-dispatch)
(defmethod drop-global-temp-view [String]
  [^String view-name]
  (drop-global-temp-view @defaults/spark view-name))
(defmethod drop-global-temp-view [SparkSession String]
  [^SparkSession spark ^String view-name]
  (drop-global-temp-view (catalog spark) view-name))
(defmethod drop-global-temp-view [Catalog String]
  [^Catalog catalog ^String view-name]
  (. catalog dropGlobalTempView view-name))


(defmulti cached? ^Boolean catalog-dispatch)
(defmethod cached? [String]
  [^String table-name]
  (cached? @defaults/spark table-name))
(defmethod cached? [SparkSession String]
  [^SparkSession spark ^String table-name]
  (cached? (catalog spark) table-name))
(defmethod cached? [Catalog String]
  [^Catalog catalog ^String table-name]
  (. catalog isCached table-name))


(defmulti list-columns ^Dataset catalog-dispatch)
(defmethod list-columns [String]
  [^String table-name]
  (list-columns @defaults/spark table-name))
(defmethod list-columns [String String]
  [^String db-name ^String table-name]
  (list-columns @defaults/spark db-name table-name))
(defmethod list-columns [SparkSession String]
  [^SparkSession spark ^String table-name]
  (list-columns (catalog spark) table-name))
(defmethod list-columns [SparkSession String String]
  [^SparkSession spark ^String db-name ^String table-name]
  (list-columns (catalog spark) db-name table-name))
(defmethod list-columns [Catalog String]
  [^Catalog catalog ^String table-name]
  (. catalog listColumns table-name))
(defmethod list-columns [Catalog String String]
  [^Catalog catalog ^String db-name ^String table-name]
  (. catalog listColumns db-name table-name))


(defmulti list-databases ^Dataset catalog-dispatch)
(defmethod list-databases []
  []
  (list-databases @defaults/spark))
(defmethod list-databases [SparkSession]
  [^SparkSession spark]
  (list-databases (catalog spark)))
(defmethod list-databases [Catalog]
  [^Catalog catalog]
  (. catalog listDatabases))


(defmulti list-tables ^Dataset catalog-dispatch)
(defmethod list-tables []
  []
  (list-tables @defaults/spark))
(defmethod list-tables [SparkSession]
  [^SparkSession spark]
  (list-tables (catalog spark)))
(defmethod list-tables [Catalog]
  [^Catalog catalog]
  (. catalog listTables))
(defmethod list-tables [String]
  [^String db-name]
  (list-tables @defaults/spark db-name))
(defmethod list-tables [SparkSession String]
  [^SparkSession spark ^String db-name]
  (list-tables (catalog spark) db-name))
(defmethod list-tables [Catalog String]
  [^Catalog catalog ^String db-name]
  (. catalog listTables db-name))


(defmulti recover-partitions catalog-dispatch)
(defmethod recover-partitions [String]
  [^String table-name]
  (recover-partitions @defaults/spark table-name))
(defmethod recover-partitions [SparkSession String]
  [^SparkSession spark ^String table-name]
  (recover-partitions (catalog spark) table-name))
(defmethod recover-partitions [Catalog String]
  [^Catalog catalog ^String table-name]
  (. catalog recoverPartitions table-name))


(defmulti refresh-by-path catalog-dispatch)
(defmethod refresh-by-path [String]
  [^String path]
  (refresh-by-path @defaults/spark path))
(defmethod refresh-by-path [SparkSession String]
  [^SparkSession spark ^String path]
  (refresh-by-path (catalog spark) path))
(defmethod refresh-by-path [Catalog String]
  [^Catalog catalog ^String path]
  (. catalog refreshByPath path))


(defmulti refresh-table catalog-dispatch)
(defmethod refresh-table [String]
  [^String table-name]
  (refresh-table @defaults/spark table-name))
(defmethod refresh-table [SparkSession String]
  [^SparkSession spark ^String table-name]
  (refresh-table (catalog spark) table-name))
(defmethod refresh-table [Catalog String]
  [^Catalog catalog ^String table-name]
  (. catalog refreshTable table-name))


(defmulti set-current-database catalog-dispatch)
(defmethod set-current-database [String]
  [^String db-name]
  (set-current-database @defaults/spark db-name))
(defmethod set-current-database [SparkSession String]
  [^SparkSession spark ^String db-name]
  (set-current-database (catalog spark) db-name))
(defmethod set-current-database [Catalog String]
  [^Catalog catalog ^String db-name]
  (. catalog setCurrentDatabase db-name))


(defmulti table-exists? ^Boolean catalog-dispatch)
(defmethod table-exists? [String]
  [^String table-name]
  (table-exists? @defaults/spark table-name))
(defmethod table-exists? [String String]
  [^String db-name ^String table-name]
  (table-exists? @defaults/spark db-name table-name))
(defmethod table-exists? [SparkSession String]
  [^SparkSession spark ^String table-name]
  (table-exists? (catalog spark) table-name))
(defmethod table-exists? [SparkSession String String]
  [^SparkSession spark ^String db-name ^String table-name]
  (table-exists? (catalog spark) db-name table-name))
(defmethod table-exists? [Catalog String]
  [^Catalog catalog ^String table-name]
  (. catalog tableExists table-name))
(defmethod table-exists? [Catalog String String]
  [^Catalog catalog ^String db-name ^String table-name]
  (. catalog tableExists db-name table-name))


(defmulti uncache-table catalog-dispatch)
(defmethod uncache-table [String]
  [^String table-name]
  (uncache-table @defaults/spark table-name))
(defmethod uncache-table [SparkSession String]
  [^SparkSession spark ^String table-name]
  (uncache-table (catalog spark) table-name))
(defmethod uncache-table [Catalog String]
  [^Catalog catalog ^String table-name]
  (. catalog uncacheTable table-name))


(defmulti drop-relation catalog-dispatch)
(defmethod drop-relation [Keyword String]
  [^Keyword relation-type ^String table-name]
  (drop-relation @defaults/spark relation-type table-name false))
(defmethod drop-relation [Keyword String Boolean]
  [^Keyword relation-type ^String table-name ^Boolean if-exists]
  (drop-relation @defaults/spark relation-type table-name if-exists))
(defmethod drop-relation [Keyword String String]
  [^Keyword relation-type ^String db-name ^String table-name]
  (drop-relation @defaults/spark relation-type db-name table-name false))
(defmethod drop-relation [Keyword String String Boolean]
  [^Keyword relation-type ^String db-name ^String table-name ^Boolean if-exists]
  (drop-relation @defaults/spark relation-type db-name table-name if-exists))
(defmethod drop-relation [SparkSession Keyword String]
  [^SparkSession spark ^Keyword relation-type ^String table-name]
  (drop-relation spark relation-type table-name false))
(defmethod drop-relation [SparkSession Keyword String Boolean]
  [^SparkSession spark ^Keyword relation-type ^String table-name ^Boolean if-exists]
  (-> spark
      (. sql (str "DROP "
                  (name relation-type) " "
                  (when if-exists "IF EXISTS ")
                  table-name))))
(defmethod drop-relation [SparkSession Keyword String String]
  [^SparkSession spark ^Keyword relation-type ^String db-name ^String table-name]
  (drop-relation spark relation-type (table-identifier db-name table-name) false))
(defmethod drop-relation [SparkSession Keyword String String Boolean]
  [^SparkSession spark ^Keyword relation-type ^String db-name ^String table-name ^Boolean if-exists]
  (drop-relation spark relation-type (table-identifier db-name table-name) if-exists))


(defmulti drop-table catalog-dispatch)
(defmethod drop-table [String]
  [^String table-name]
  (drop-table @defaults/spark table-name false))
(defmethod drop-table [String Boolean]
  [^String table-name ^Boolean if-exists]
  (drop-table @defaults/spark table-name if-exists))
(defmethod drop-table [String String]
  [^String db-name ^String table-name]
  (drop-table @defaults/spark db-name table-name false))
(defmethod drop-table [String String Boolean]
  [^String db-name ^String table-name ^Boolean if-exists]
  (drop-table @defaults/spark db-name table-name if-exists))
(defmethod drop-table [SparkSession String]
  [^SparkSession spark ^String table-name]
  (drop-relation spark :TABLE table-name false))
(defmethod drop-table [SparkSession String Boolean]
  [^SparkSession spark ^String table-name ^Boolean if-exists]
  (drop-relation spark :TABLE table-name if-exists))
(defmethod drop-table [SparkSession String String]
  [^SparkSession spark ^String db-name ^String table-name]
  (drop-relation spark :TABLE db-name table-name false))
(defmethod drop-table [SparkSession String String Boolean]
  [^SparkSession spark ^String db-name ^String table-name ^Boolean if-exists]
  (drop-relation spark :TABLE db-name table-name if-exists))


(defmulti drop-view catalog-dispatch)
(defmethod drop-view [String]
  [^String table-name]
  (drop-view @defaults/spark table-name false))
(defmethod drop-view [String Boolean]
  [^String table-name ^Boolean if-exists]
  (drop-view @defaults/spark table-name if-exists))
(defmethod drop-view [String String]
  [^String db-name ^String table-name]
  (drop-view @defaults/spark db-name table-name false))
(defmethod drop-view [String String Boolean]
  [^String db-name ^String table-name ^Boolean if-exists]
  (drop-view @defaults/spark db-name table-name if-exists))
(defmethod drop-view [SparkSession String]
  [^SparkSession spark ^String table-name]
  (drop-relation spark :VIEW table-name false))
(defmethod drop-view [SparkSession String Boolean]
  [^SparkSession spark ^String table-name ^Boolean if-exists]
  (drop-relation spark :VIEW table-name if-exists))
(defmethod drop-view [SparkSession String String]
  [^SparkSession spark ^String db-name ^String table-name]
  (drop-relation spark :VIEW db-name table-name false))
(defmethod drop-view [SparkSession String String Boolean]
  [^SparkSession spark ^String db-name ^String table-name ^Boolean if-exists]
  (drop-relation spark :VIEW db-name table-name if-exists))
