(ns zero-one.geni.catalog
  "Spark's catalog of databases, tables, views and their caching. Each
  function takes the catalog first, as a SparkSession or a Catalog, or
  leaves it out for the default session's: `(cached? \"tbl\")`,
  `(cached? spark \"tbl\")`."
  (:require [clojure.string :as string]
            [zero-one.geni.defaults :as defaults])
  (:import (org.apache.spark.sql.catalog Catalog)
           (org.apache.spark.sql SparkSession)))

(defn table-identifier
  [database name] (str database "." name))

(defn catalog ^Catalog
  ([]
   (catalog @defaults/spark))
  ([^SparkSession spark]
   (. spark catalog)))

(defn- split-catalog
  "The catalog that `args` start with, a Catalog or a SparkSession's, or the
  default session's, and the rest of `args`."
  [args]
  (let [[x & more] args]
    (condp instance? x
      Catalog      [x more]
      SparkSession [(catalog x) more]
      [(catalog) args])))

(defn cache-table [& args]
  (let [[^Catalog c [table-name storage-level]] (split-catalog args)]
    (if storage-level
      (.cacheTable c table-name storage-level)
      (.cacheTable c table-name))))

(defn clear-cache [& args]
  (.clearCache ^Catalog (first (split-catalog args))))

(defn current-database ^String [& args]
  (.currentDatabase ^Catalog (first (split-catalog args))))

(defn current-catalog
  "Returns the name of the session's current catalog, such as
  `\"spark_catalog\"`, Spark's built-in one."
  ^String [& args]
  (.currentCatalog ^Catalog (first (split-catalog args))))

(defn set-current-catalog
  "Makes the catalog `catalog-name` the session's current one, for the table
  names that don't name a catalog. A catalog other than Spark's built-in one
  comes from a `spark.sql.catalog.<name>` config."
  [& args]
  (let [[^Catalog c [catalog-name]] (split-catalog args)]
    (.setCurrentCatalog c catalog-name)))

(defn list-catalogs
  "Returns a Dataset of the session's catalogs, with a `name` and a
  `description` column, or only those whose names match the pattern, where
  `*` matches any characters."
  [& args]
  (let [[^Catalog c [pattern]] (split-catalog args)]
    (if pattern (.listCatalogs c pattern) (.listCatalogs c))))

(defn database-exists? [& args]
  (let [[^Catalog c [db-name]] (split-catalog args)]
    (.databaseExists c db-name)))

(defn drop-temp-view [& args]
  (let [[^Catalog c [view-name]] (split-catalog args)]
    (.dropTempView c view-name)))

(defn drop-global-temp-view [& args]
  (let [[^Catalog c [view-name]] (split-catalog args)]
    (.dropGlobalTempView c view-name)))

(defn cached? [& args]
  (let [[^Catalog c [table-name]] (split-catalog args)]
    (.isCached c table-name)))

(defn list-columns
  "The columns of a table, by name or by database and name."
  [& args]
  (let [[^Catalog c [x table-name]] (split-catalog args)]
    (if table-name (.listColumns c x table-name) (.listColumns c x))))

(defn list-databases [& args]
  (.listDatabases ^Catalog (first (split-catalog args))))

(defn list-tables
  "The tables of the current database, or of `db-name`."
  [& args]
  (let [[^Catalog c [db-name]] (split-catalog args)]
    (if db-name (.listTables c db-name) (.listTables c))))

(defn recover-partitions [& args]
  (let [[^Catalog c [table-name]] (split-catalog args)]
    (.recoverPartitions c table-name)))

(defn refresh-by-path [& args]
  (let [[^Catalog c [path]] (split-catalog args)]
    (.refreshByPath c path)))

(defn refresh-table [& args]
  (let [[^Catalog c [table-name]] (split-catalog args)]
    (.refreshTable c table-name)))

(defn set-current-database [& args]
  (let [[^Catalog c [db-name]] (split-catalog args)]
    (.setCurrentDatabase c db-name)))

(defn table-exists?
  "Whether a table or a view exists, by name or by database and name."
  [& args]
  (let [[^Catalog c [x table-name]] (split-catalog args)]
    (if table-name (.tableExists c x table-name) (.tableExists c x))))

(defn uncache-table [& args]
  (let [[^Catalog c [table-name]] (split-catalog args)]
    (.uncacheTable c table-name)))

(defn drop-relation
  "Drops a relation of `relation-type`, `:TABLE` or `:VIEW`, by name or by
  database and name, and with `if-exists` true, only when it exists. A
  SparkSession can come first."
  [& args]
  (let [[spark [relation-type & names]] (defaults/session-and-args args)
        [names if-exists]               (if (boolean? (last names))
                                          [(butlast names) (last names)]
                                          [names false])]
    (.sql ^SparkSession spark (str "DROP " (name relation-type) " "
                                   (when if-exists "IF EXISTS ")
                                   (string/join "." names)))))

(defn- drop-of-type [relation-type args]
  (let [[spark args] (defaults/session-and-args args)]
    (apply drop-relation spark relation-type args)))

(defn drop-table
  "Drops a table, by name or by database and name, and with `if-exists`
  true, only when it exists. A SparkSession can come first."
  [& args]
  (drop-of-type :TABLE args))

(defn drop-view
  "Drops a view, by name or by database and name, and with `if-exists` true,
  only when it exists. A SparkSession can come first."
  [& args]
  (drop-of-type :VIEW args))
