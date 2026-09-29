# CB-01: Reading and Writing Datasets

As in the [Pandas Cookbook](https://nbviewer.jupyter.org/github/jvns/pandas-cookbook/blob/master/cookbook/Chapter%201%20-%20Reading%20from%20a%20CSV.ipynb), we are going to use the Montréal cyclists data, a copy of which is in the Pandas Cookbook's repo. Every part of this cookbook starts with the same setup: Geni's core namespace, and a function that downloads a dataset once.

```clojure
(require '[clojure.java.io :as io])
(require '[zero-one.geni.core :as g])

(defn download-data! [source-url target-path]
  (if (.exists (io/file target-path))
    :already-exists
    (do
      (io/make-parents target-path)
      (with-open [in (io/input-stream source-url)]
        (io/copy in (io/file target-path)))
      :downloaded)))
```

And actually download the data:

```clojure
(def bikes-data-url
  "https://raw.githubusercontent.com/jvns/pandas-cookbook/80817f5a46ef6d2f8444eeb7181e00c0aabb5a57/cookbook/data/bikes.csv")

(def bikes-data-path "data/cookbook/bikes.csv")

(download-data! bikes-data-url bikes-data-path)
; :downloaded
```

## 1.1 Reading Data from a CSV File

In most cases, we can read CSV data correctly with the default `g/read-csv!` function. However, in this case, we run into a couple of issues:

```clojure
(def broken-df (g/read-csv! bikes-data-path))

(-> broken-df (g/limit 3) g/show)
;; =stdout=>
; +-----------------------------------------------------------------------------------------------------------------------------------------------------------------+
; |Date;Berri 1;Br�beuf (donn�es non disponibles);C�te-Sainte-Catherine;Maisonneuve 1;Maisonneuve 2;du Parc;Pierre-Dupuy;Rachel1;St-Urbain (donn�es non disponibles)|
; +-----------------------------------------------------------------------------------------------------------------------------------------------------------------+
; |01/01/2012;35;;0;38;51;26;10;16;                                                                                                                                 |
; |02/01/2012;83;;1;68;153;53;6;43;                                                                                                                                 |
; |03/01/2012;135;;2;104;248;89;3;58;                                                                                                                               |
; +-----------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

Firstly, each line on the CSV file is read as a single column. This is due to the misreading of the CSV delimiter or separator. By default, `g/read-csv!` looks for a comma, whereas this file uses the semicolon as the delimiter. Secondly, the column names contain accented French characters that are not read properly. We fix the two issues by passing the `:delimiter` and `:encoding` options to `g/read-csv!`:

```clojure
(def fixed-df
  (g/read-csv! bikes-data-path {:delimiter ";" :encoding "ISO-8859-1"}))

(-> fixed-df (g/limit 3) g/show)
;; =stdout=>
; +----------+-------+---------------------------------+---------------------+-------------+-------------+-------+------------+-------+-----------------------------------+
; |Date      |Berri 1|Brébeuf (données non disponibles)|Côte-Sainte-Catherine|Maisonneuve 1|Maisonneuve 2|du Parc|Pierre-Dupuy|Rachel1|St-Urbain (données non disponibles)|
; +----------+-------+---------------------------------+---------------------+-------------+-------------+-------+------------+-------+-----------------------------------+
; |01/01/2012|35     |NULL                             |0                    |38           |51           |26     |10          |16     |NULL                               |
; |02/01/2012|83     |NULL                             |1                    |68           |153          |53     |6           |43     |NULL                               |
; |03/01/2012|135    |NULL                             |2                    |104          |248          |89     |3           |58     |NULL                               |
; +----------+-------+---------------------------------+---------------------+-------------+-------------+-------+------------+-------+-----------------------------------+
```

It appears that we have fixed the issues! But it is still quite difficult to read a wide table. We can view the data vertically using `g/show-vertical`:

```clojure
(-> fixed-df (g/limit 3) g/show-vertical)
; -RECORD 0-----------------------------------------
;  Date                                | 01/01/2012
;  Berri 1                             | 35
;  Brébeuf (données non disponibles)   | NULL
;  Côte-Sainte-Catherine               | 0
;  Maisonneuve 1                       | 38
;  Maisonneuve 2                       | 51
;  du Parc                             | 26
;  Pierre-Dupuy                        | 10
;  Rachel1                             | 16
;  St-Urbain (données non disponibles) | NULL
; -RECORD 1-----------------------------------------
;  Date                                | 02/01/2012
;  Berri 1                             | 83
;  Brébeuf (données non disponibles)   | NULL
;  Côte-Sainte-Catherine               | 1
;  Maisonneuve 1                       | 68
;  Maisonneuve 2                       | 153
;  du Parc                             | 53
;  Pierre-Dupuy                        | 6
;  Rachel1                             | 43
;  St-Urbain (données non disponibles) | NULL
; -RECORD 2-----------------------------------------
;  Date                                | 03/01/2012
;  Berri 1                             | 135
;  Brébeuf (données non disponibles)   | NULL
;  Côte-Sainte-Catherine               | 2
;  Maisonneuve 1                       | 104
;  Maisonneuve 2                       | 248
;  du Parc                             | 89
;  Pierre-Dupuy                        | 3
;  Rachel1                             | 58
;  St-Urbain (données non disponibles) | NULL
```

We may also like to inspect the inferred schema of the dataset and count the number of rows:

```clojure
(g/print-schema fixed-df)
;; =stdout=>
; root
;  |-- Date: string (nullable = true)
;  |-- Berri 1: integer (nullable = true)
;  |-- Brébeuf (données non disponibles): string (nullable = true)
;  |-- Côte-Sainte-Catherine: integer (nullable = true)
;  |-- Maisonneuve 1: integer (nullable = true)
;  |-- Maisonneuve 2: integer (nullable = true)
;  |-- du Parc: integer (nullable = true)
;  |-- Pierre-Dupuy: integer (nullable = true)
;  |-- Rachel1: integer (nullable = true)
;  |-- St-Urbain (données non disponibles): string (nullable = true)

(g/count fixed-df)
;; => 310
```

Finally, we can collect the Spark Dataset into a sequence of maps through `g/collect`:

```clojure
(-> fixed-df (g/limit 3) g/collect)
; => ({:du Parc 26,
;      :Rachel1 16,
;      :Pierre-Dupuy 10,
;      :Berri 1 35,
;      :Maisonneuve 1 38,
;      :Brébeuf (données non disponibles) nil,
;      :Date "01/01/2012",
;      :Côte-Sainte-Catherine 0,
;      :St-Urbain (données non disponibles) nil,
;      :Maisonneuve 2 51}
;     {:du Parc 53,
;      :Rachel1 43,
;      :Pierre-Dupuy 6,
;      :Berri 1 83,
;      :Maisonneuve 1 68,
;      :Brébeuf (données non disponibles) nil,
;      :Date "02/01/2012",
;      :Côte-Sainte-Catherine 1,
;      :St-Urbain (données non disponibles) nil,
;      :Maisonneuve 2 153}
;     {:du Parc 89,
;      :Rachel1 58,
;      :Pierre-Dupuy 3,
;      :Berri 1 135,
;      :Maisonneuve 1 104,
;      :Brébeuf (données non disponibles) nil,
;      :Date "03/01/2012",
;      :Côte-Sainte-Catherine 2,
;      :St-Urbain (données non disponibles) nil,
;      :Maisonneuve 2 248})
```

We can see that the column names are keywordised, which may not play so well with non-kebab-case column names. In the next sub-section, we'll see how to address this.

## 1.2 Selecting and Renaming Columns

Suppose we would like to view only the date and Berri-1 column, we could do this through `g/select`:

```clojure
(-> fixed-df
    (g/select :Date "Berri 1")
    (g/limit 3)
    g/show)
;; =stdout=>
; +----------+-------+
; |Date      |Berri 1|
; +----------+-------+
; |01/01/2012|35     |
; |02/01/2012|83     |
; |03/01/2012|135    |
; +----------+-------+
```

The function `g/select` may take strings, keywords and symbols as arguments to refer to column names. As a mental model, we can think of a dataset as a sequence of maps, and the keys of associative maps are usually keywords by convention. For that reason, idiomatic Geni prefers the use of keywords to strings and symbols.

It is thus preferable to work with kebab-case column names (unlike `:Brébeuf (données non disponibles)` as it contains spaces, parentheses and less importantly capital letters and special characters). One way to rename the columns is to use `g/select` with a map:

```clojure
(-> fixed-df
    (g/select {:date "Date" :berri-1 "Berri 1"})
    (g/limit 3)
    g/show)
;; =stdout=>
; +----------+-------+
; |date      |berri-1|
; +----------+-------+
; |01/01/2012|35     |
; |02/01/2012|83     |
; |03/01/2012|135    |
; +----------+-------+
```

However, in this case, it can be easier to re-set all the column names using `g/to-df` particularly after loading a dataset:

```clojure
(def renamed-df
  (-> fixed-df
      (g/to-df :date
               :berri-1
               :brebeuf
               :cote-sainte-catherine
               :maisonneuve-1
               :maisonneuve-2
               :du-parc
               :pierre-dupuy
               :rachel-1
               :st-urbain)))

(-> renamed-df (g/limit 3) g/show)
;; =stdout=>
; +----------+-------+-------+---------------------+-------------+-------------+-------+------------+--------+---------+
; |date      |berri-1|brebeuf|cote-sainte-catherine|maisonneuve-1|maisonneuve-2|du-parc|pierre-dupuy|rachel-1|st-urbain|
; +----------+-------+-------+---------------------+-------------+-------------+-------+------------+--------+---------+
; |01/01/2012|35     |NULL   |0                    |38           |51           |26     |10          |16      |NULL     |
; |02/01/2012|83     |NULL   |1                    |68           |153          |53     |6           |43      |NULL     |
; |03/01/2012|135    |NULL   |2                    |104          |248          |89     |3           |58      |NULL     |
; +----------+-------+-------+---------------------+-------------+-------------+-------+------------+--------+---------+
```

## 1.3 Describing Columns

We can get descriptions of numeric columns using `g/describe`:

```clojure
(-> renamed-df
    g/describe
    g/show)
;; =stdout=>
; +-------+----------+-----------------+-------+---------------------+------------------+-----------------+------------------+------------------+------------------+---------+
; |summary|date      |berri-1          |brebeuf|cote-sainte-catherine|maisonneuve-1     |maisonneuve-2    |du-parc           |pierre-dupuy      |rachel-1          |st-urbain|
; +-------+----------+-----------------+-------+---------------------+------------------+-----------------+------------------+------------------+------------------+---------+
; |count  |310       |310              |0      |310                  |310               |310              |310               |310               |310               |0        |
; |mean   |NULL      |2985.048387096774|NULL   |1233.3516129032257   |1983.3258064516128|3510.261290322581|1862.983870967742 |1054.3064516129032|2873.483870967742 |NULL     |
; |stddev |NULL      |2169.271061762149|NULL   |944.6431881884916    |1450.715170237464 |2484.959788723178|1332.5432662293993|1064.0292047222817|2039.3155043485128|NULL     |
; |min    |01/01/2012|32               |NULL   |0                    |33                |47               |18                |0                 |0                 |NULL     |
; |max    |31/10/2012|7077             |NULL   |3124                 |4999              |8222             |4510              |4386              |6595              |NULL     |
; +-------+----------+-----------------+-------+---------------------+------------------+-----------------+------------------+------------------+------------------+---------+
```

## 1.4 Writing Datasets

Writing datasets to file is straightforward. Spark [encourages the use of parquet](https://databricks.com/glossary/what-is-parquet) formats. To write to parquet, we can invoke `g/write-parquet!`:

```clojure
(g/write-parquet! renamed-df "data/cookbook/bikes.parquet" {:mode "overwrite"})
```

Analogous read and write functions are available. For instance, `g/write-avro!` to write as an Avro file and `g/read-json!` to read a JSON file.
