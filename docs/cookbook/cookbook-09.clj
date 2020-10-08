(ns geni.cookbook-09
  (:require
   [clojure.java.io]
   [clojure.java.shell]
   [zero-one.geni.core :as g]
   [zero-one.geni.ml :as ml]))

(defn download-data! [source-url target-path]
  (if (-> target-path clojure.java.io/file .exists)
    :already-exists
    (do
      (clojure.java.io/make-parents target-path)
      (clojure.java.shell/sh "wget" "-O" target-path source-url)
      :downloaded)))

;; Part 9: Loading Data From SQL Databases

(download-data!
 "https://cdn.sqlitetutorial.net/wp-content/uploads/2018/03/chinook.zip"
 "data/chinook.zip")

(when-not (-> "data/chinook.db" clojure.java.io/file .exists)
  (clojure.java.shell/sh "unzip" "data/chinook.zip" "-d" "data/"))

;; 9.1 Reading From SQLite

(def chinook-tracks
  (g/read-jdbc! {:driver        "org.sqlite.JDBC"
                 :url           "jdbc:sqlite:data/chinook.db"
                 :dbtable       "tracks"
                 :kebab-columns true}))

(g/count chinook-tracks)

(g/print-schema chinook-tracks)

(g/show chinook-tracks {:num-rows 3})

;; 9.2 Writing to SQLite

(g/write-jdbc! chinook-tracks
               {:driver  "org.sqlite.JDBC"
                :url     "jdbc:sqlite:data/chinook-tracks.sqlite"
                :dbtable "tracks"})
