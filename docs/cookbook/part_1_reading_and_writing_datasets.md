# Cookbook 1: Reading and Writing Datasets

As in the [Pandas Cookbook](https://nbviewer.jupyter.org/github/jvns/pandas-cookbook/blob/master/cookbook/Chapter%201%20-%20Reading%20from%20a%20CSV.ipynb), we are going to use the Montréal cyclists data, which is freely available [here](http://donnees.ville.montreal.qc.ca/dataset/velos-comptage). To download the dataset, we use the following setup:

```clojure
(ns geni.cookbook-code
  (:require
    [clojure.java.io]
    [clojure.java.shell]
    [zero-one.geni.core :as g]))

(defn download-data! [source-url target-path]
  (if (-> target-path clojure.java.io/file .exists)
    :already-exists
    (do
      (clojure.java.io/make-parents target-path)
      (clojure.java.shell/sh "wget" "-O" target-path source-url)
      :downloaded)))
```

And actually download the data:

```clojure
(def bikes-data-url "https://raw.githubusercontent.com/jvns/pandas-cookbook/master/data/bikes.csv")
(def bikes-data-path "target/cookbook/bikes.csv")
(download-data! bikes-data-url bikes-data-path)
=> :downloaded
```

## 1.1 Creating a Spark Session

To read datasets from any source, we must first create a Spark session. Spark is typically used for large-scale distributed computing, but in our case, we are only going to be looking at smaller datasets. Therefore, the default single-node Spark session will do the job:

```clojure
(defonce spark (g/create-spark-session {}))

(g/spark-conf spark)
=> {:spark.app.name "Geni App",
    :spark.driver.host ...,
    :spark.app.id "local-1595132475689",
    :spark.master "local[*]",
    :spark.executor.id "driver",
    :spark.driver.port "64818"}
```

We see that the value of `:spark.master` is `local[*]`. This means that the session will run on a single node with all available cores.

## 1.2 Reading Data from a CSV File

In most cases, we can read CSV data correctly with the default `g/read-csv!` function. However, in this case, we run into a couple of issues:

```clojure
(def broken-df (g/read-csv! spark bikes-data-path))

(-> broken-df (g/limit 3) g/show)
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
  (g/read-csv! spark bikes-data-path {:delimiter ";"
                                      :encoding "ISO-8859-1"
                                      :inferSchema "true"}))

(-> fixed-df (g/limit 3) g/show)
; +----------+-------+---------------------------------+---------------------+-------------+-------------+-------+------------+-------+-----------------------------------+
; |Date      |Berri 1|Brébeuf (données non disponibles)|Côte-Sainte-Catherine|Maisonneuve 1|Maisonneuve 2|du Parc|Pierre-Dupuy|Rachel1|St-Urbain (données non disponibles)|
; +----------+-------+---------------------------------+---------------------+-------------+-------------+-------+------------+-------+-----------------------------------+
; |01/01/2012|35     |null                             |0                    |38           |51           |26     |10          |16     |null                               |
; |02/01/2012|83     |null                             |1                    |68           |153          |53     |6           |43     |null                               |
; |03/01/2012|135    |null                             |2                    |104          |248          |89     |3           |58     |null                               |
; +----------+-------+---------------------------------+---------------------+-------------+-------------+-------+------------+-------+-----------------------------------+
```

Note that we also added the option `:inferSchema` to turn on automatic schema inference. It appears that we have fixed the issues! But it is still quite difficult to read a wide table. We can view the data vertically using `g/show-vertical`:

```clojure
(-> fixed-df (g/limit 3) g/show-vertical)
; -RECORD 0-----------------------------------------
;  Date                                | 01/01/2012
;  Berri 1                             | 35
;  Brébeuf (données non disponibles)   | null
;  Côte-Sainte-Catherine               | 0
;  Maisonneuve 1                       | 38
;  Maisonneuve 2                       | 51
;  du Parc                             | 26
;  Pierre-Dupuy                        | 10
;  Rachel1                             | 16
;  St-Urbain (données non disponibles) | null
; -RECORD 1-----------------------------------------
;  Date                                | 02/01/2012
;  Berri 1                             | 83
;  Brébeuf (données non disponibles)   | null
;  Côte-Sainte-Catherine               | 1
;  Maisonneuve 1                       | 68
;  Maisonneuve 2                       | 153
;  du Parc                             | 53
;  Pierre-Dupuy                        | 6
;  Rachel1                             | 43
;  St-Urbain (données non disponibles) | null
; -RECORD 2-----------------------------------------
;  Date                                | 03/01/2012
;  Berri 1                             | 135
;  Brébeuf (données non disponibles)   | null
;  Côte-Sainte-Catherine               | 2
;  Maisonneuve 1                       | 104
;  Maisonneuve 2                       | 248
;  du Parc                             | 89
;  Pierre-Dupuy                        | 3
;  Rachel1                             | 58
;  St-Urbain (données non disponibles) | null
```

We may also like to inspect the inferred schema of the dataset and count the number of rows:

```clojure
(g/print-schema fixed-df)
; root
;  |-- Date: string (nullable = true)
;  |-- Berri 1: string (nullable = true)
;  |-- Brébeuf (données non disponibles): string (nullable = true)
;  |-- Côte-Sainte-Catherine: string (nullable = true)
;  |-- Maisonneuve 1: string (nullable = true)
;  |-- Maisonneuve 2: string (nullable = true)
;  |-- du Parc: string (nullable = true)
;  |-- Pierre-Dupuy: string (nullable = true)
;  |-- Rachel1: string (nullable = true)
;  |-- St-Urbain (données non disponibles): string (nullable = true)

(g/count fixed-df)
=> 310
```

Finally, we can collect the Spark Dataset into a sequence of maps through `g/collect`:

```clojure
(-> fixed-df (g/limit 3) g/collect)
=> ({:du Parc "26",
     :Rachel1 "16",
     :Pierre-Dupuy "10",
     :Berri 1 "35",
     :Maisonneuve 1 "38",
     :Brébeuf (données non disponibles) nil,
     :Date "01/01/2012",
     :Côte-Sainte-Catherine "0",
     :St-Urbain (données non disponibles) nil,
     :Maisonneuve 2 "51"}
    {:du Parc "53",
     :Rachel1 "43",
     :Pierre-Dupuy "6",
     :Berri 1 "83",
     :Maisonneuve 1 "68",
     :Brébeuf (données non disponibles) nil,
     :Date "02/01/2012",
     :Côte-Sainte-Catherine "1",
     :St-Urbain (données non disponibles) nil,
     :Maisonneuve 2 "153"}
    {:du Parc "89",
     :Rachel1 "58",
     :Pierre-Dupuy "3",
     :Berri 1 "135",
     :Maisonneuve 1 "104",
     :Brébeuf (données non disponibles) nil,
     :Date "03/01/2012",
     :Côte-Sainte-Catherine "2",
     :St-Urbain (données non disponibles) nil,
     :Maisonneuve 2 "248"})
```

We can see that the column names are keywordised, which may not play so well with non-kebab-case column names. In the next sub-section, we'll see how to address this.

## 1.2 Selecting and Renaming Columns

Suppose we would like to view only the date and Berri-1 column, we could do this through `g/select`:

```clojure
(-> fixed-df
    (g/select :Date "Berri 1")
    (g/limit 3)
    g/show)
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
; +----------+-------+
; |Date      |Berri 1|
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
; +----------+-------+-------+---------------------+-------------+-------------+-------+------------+--------+---------+
; |date      |berri-1|brebeuf|cote-sainte-catherine|maisonneuve-1|maisonneuve-2|du-parc|pierre-dupuy|rachel-1|st-urbain|
; +----------+-------+-------+---------------------+-------------+-------------+-------+------------+--------+---------+
; |01/01/2012|35     |null   |0                    |38           |51           |26     |10          |16      |null     |
; |02/01/2012|83     |null   |1                    |68           |153          |53     |6           |43      |null     |
; |03/01/2012|135    |null   |2                    |104          |248          |89     |3           |58      |null     |
; +----------+-------+-------+---------------------+-------------+-------------+-------+------------+--------+---------+
```

## 1.3 Describing Columns

We can get descriptions of numeric columns using `g/describe`:

```clojure
(-> renamed-df
    g/describe
    g/show)
; +-------+----------+-----------------+-------+---------------------+------------------+-----------------+------------------+------------------+------------------+---------+
; |summary|date      |berri-1          |brebeuf|cote-sainte-catherine|maisonneuve-1     |maisonneuve-2    |du-parc           |pierre-dupuy      |rachel-1          |st-urbain|
; +-------+----------+-----------------+-------+---------------------+------------------+-----------------+------------------+------------------+------------------+---------+
; |count  |310       |310              |0      |310                  |310               |310              |310               |310               |310               |0        |
; |mean   |null      |2985.048387096774|null   |1233.3516129032257   |1983.3258064516128|3510.261290322581|1862.983870967742 |1054.3064516129032|2873.483870967742 |null     |
; |stddev |null      |2169.271061762149|null   |944.6431881884916    |1450.715170237464 |2484.959788723178|1332.5432662293993|1064.0292047222817|2039.3155043485128|null     |
; |min    |01/01/2012|32               |null   |0                    |33                |47               |18                |0                 |0                 |null     |
; |max    |31/10/2012|7077             |null   |3124                 |4999              |8222             |4510              |4386              |6595              |null     |
; +-------+----------+-----------------+-------+---------------------+------------------+-----------------+------------------+------------------+------------------+---------+
```

## 1.4 Writing Datasets

Writing datasets to file is straightforward. Spark [encourages the use of parquet](https://databricks.com/glossary/what-is-parquet) formats. To write to parquet, we can invoke `g/write-parquet!`:

```clojure
(g/write-parquet! renamed-df "target/cookbook/bikes.parquet"))
```

Analogous read and write functions are available. For instance, `g/write-avro!` to write as an Avro file and `g/read-json!` to read a JSON file.
