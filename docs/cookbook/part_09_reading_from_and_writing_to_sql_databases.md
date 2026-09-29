# CB-09: Reading From and Writing To SQL Databases

In this part of the cookbook, we use the [Chinook sample SQLite database](https://github.com/lerocha/chinook-database). We download the v1.4.5 release into `data/cookbook`. Start the REPL with `clj -M:spark:test` to include the SQLite JDBC driver.

```clojure
(require '[clojure.java.io :as io])
(require '[zero-one.geni.core :as g])
(when-not (.exists (io/file "data/cookbook/chinook.db"))
  (io/make-parents "data/cookbook/chinook.db")
  (with-open [in (io/input-stream "https://github.com/lerocha/chinook-database/releases/download/v1.4.5/Chinook_Sqlite.sqlite")]
    (io/copy in (io/file "data/cookbook/chinook.db"))))
```

## 9.1 Reading From SQLite

Reading from databases through JDBC is slightly different to reading from a file. In particular, we must specify `:driver`, `:url` and `:dbtable`. In the case of SQLite, we can load the table as follows:

```clojure
(def chinook-tracks
  (g/read-jdbc! {:driver        "org.sqlite.JDBC"
                 :url           "jdbc:sqlite:data/cookbook/chinook.db"
                 :dbtable       "Track"
                 :kebab-columns true}))

(g/count chinook-tracks)
;; => 3503

(g/print-schema chinook-tracks)
;; =stdout=>
; root
;  |-- TrackId: integer (nullable = true)
;  |-- Name: string (nullable = true)
;  |-- AlbumId: integer (nullable = true)
;  |-- MediaTypeId: integer (nullable = true)
;  |-- GenreId: integer (nullable = true)
;  |-- Composer: string (nullable = true)
;  |-- Milliseconds: integer (nullable = true)
;  |-- Bytes: integer (nullable = true)
;  |-- UnitPrice: decimal(10,2) (nullable = true)

(g/show chinook-tracks {:num-rows 3})
;; =stdout=>
; +-------+---------------------------------------+-------+-----------+-------+---------------------------------------------------+------------+--------+---------+
; |TrackId|Name                                   |AlbumId|MediaTypeId|GenreId|Composer                                           |Milliseconds|Bytes   |UnitPrice|
; +-------+---------------------------------------+-------+-----------+-------+---------------------------------------------------+------------+--------+---------+
; |1      |For Those About To Rock (We Salute You)|1      |1          |1      |Angus Young, Malcolm Young, Brian Johnson          |343719      |11170334|0.99     |
; |2      |Balls to the Wall                      |2      |2          |1      |NULL                                               |342562      |5510424 |0.99     |
; |3      |Fast As a Shark                        |3      |2          |1      |F. Baltes, S. Kaufman, U. Dirkscneider & W. Hoffman|230619      |3990994 |0.99     |
; +-------+---------------------------------------+-------+-----------+-------+---------------------------------------------------+------------+--------+---------+
; only showing top 3 rows
```

## 9.2 Writing to SQLite

Writing to SQLite databases has a similar format to reading it:

```clojure
(g/write-jdbc! chinook-tracks
               {:driver  "org.sqlite.JDBC"
                :url     "jdbc:sqlite:data/cookbook/chinook-tracks.sqlite"
                :dbtable "tracks"
                :mode "overwrite"})
;; => nil
```

The drivers `"com.mysql.jdbc.Driver"` and `"org.postgresql.Driver"` can be used for MySQL and PostgreSQL respectively.
