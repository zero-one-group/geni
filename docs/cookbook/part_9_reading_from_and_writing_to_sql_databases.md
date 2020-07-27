# CB9: Reading From and Writing To SQL Databases

In this part of the cookbook, we use the well-known [Chinook sample SQLite database](https://www.sqlitetutorial.net/sqlite-sample-database/). We must first download the zipped database file and unzip it:

```clojure
(download-data!
  "https://cdn.sqlitetutorial.net/wp-content/uploads/2018/03/chinook.zip"
  "data/chinook.zip")
=> :downloaded

(when-not (-> "data/chinook.db" clojure.java.io/file .exists)
  (clojure.java.shell/sh "unzip" "data/chinook.zip" "-d" "data/"))
=> {:exit 0,
    :out
    "Archive:  data/chinook.zip\n  inflating: data/chinook.db         \n",
    :err ""}
```

## 9.1 Reading From SQLite

Reading from databases through JDBC is slightly different to reading from a file. In particular, we must specify `:driver`, `:url` and `:dbtable`. In the case of SQLite, we can load the table as follows:

```clojure
(def chinook-tracks
  (normalise-column-names
    (g/read-jdbc! spark {:driver  "org.sqlite.JDBC"
                         :url     "jdbc:sqlite:data/chinook.db"
                         :dbtable "tracks"})))

(g/count chinook-tracks)
=> 3503

(g/print-schema chinook-tracks)
; root
;  |-- track-id: integer (nullable = true)
;  |-- name: string (nullable = true)
;  |-- album-id: integer (nullable = true)
;  |-- media-type-id: integer (nullable = true)
;  |-- genre-id: integer (nullable = true)
;  |-- composer: string (nullable = true)
;  |-- milliseconds: integer (nullable = true)
;  |-- bytes: integer (nullable = true)
;  |-- unit-price: decimal(10,2) (nullable = true)

(g/show chinook-tracks {:num-rows 3})
; +--------+---------------------------------------+--------+-------------+--------+---------------------------------------------------+------------+--------+----------+
; |track-id|name                                   |album-id|media-type-id|genre-id|composer                                           |milliseconds|bytes   |unit-price|
; +--------+---------------------------------------+--------+-------------+--------+---------------------------------------------------+------------+--------+----------+
; |1       |For Those About To Rock (We Salute You)|1       |1            |1       |Angus Young, Malcolm Young, Brian Johnson          |343719      |11170334|0.99      |
; |2       |Balls to the Wall                      |2       |2            |1       |null                                               |342562      |5510424 |0.99      |
; |3       |Fast As a Shark                        |3       |2            |1       |F. Baltes, S. Kaufman, U. Dirkscneider & W. Hoffman|230619      |3990994 |0.99      |
; +--------+---------------------------------------+--------+-------------+--------+---------------------------------------------------+------------+--------+----------+
; only showing top 3 rows
```

## 9.2 Writing to SQLite

Writing to SQLite databases has a similar format to reading it:

```clojure
(g/write-jdbc! chinook-tracks
               {:driver  "org.sqlite.JDBC"
                :url     "jdbc:sqlite:data/chinook-tracks.sqlite"
                :dbtable "tracks"})
=> nil
```

The drivers `"com.mysql.jdbc.Driver"` and `"org.postgresql.Driver"` can be used for MySQL and PostgreSQL respectively.
