# Cookbook 1: Reading and Writing Datasets

As in the [Pandas Cookbook](https://nbviewer.jupyter.org/github/jvns/pandas-cookbook/blob/master/cookbook/Chapter%201%20-%20Reading%20from%20a%20CSV.ipynb), we are going to use Montréal cyclists data, which is freely available [here](http://donnees.ville.montreal.qc.ca/dataset/velos-comptage). First, we download the data:

```clojure
(ns geni.cookbook
  (:require
    [clojure.java.io]
    [clojure.java.shell]
    [zero-one.geni.core :as g]))

(def bikes-data-path "resources/cookbook/bikes.csv")

(defn download-bikes-data! []
  (let [data-url "https://raw.githubusercontent.com/jvns/pandas-cookbook/master/data/bikes.csv"]
    (if (-> bikes-data-path clojure.java.io/file .exists)
      :already-exists
      (do
        (clojure.java.io/make-parents bikes-data-path)
        (clojure.java.shell/sh "wget" "-O" bikes-data-path data-url)
        :downloaded))))

(download-bikes-data!)
=> :downloaded
```
## 1.1 Creating a Spark Session

To read datasets from any source, we must first create a Spark session. Spark is typically used for large-scale distributed computing. However, we are only going to be looking at smaller datasets, so the default local, single-node Spark will do:

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

We see that `:spark.master` is set to `local[*]`, which means that the session will run on a single node with all available cores.

## 1.2 Reading Data from a CSV File

We can read CSV files using the `g/read-csv!` function. For most cases, we can read CSV data correctly with the defaults. However, in this case, we run into a couple of issues:

```clojure
(def broken-df (g/read-csv! spark bikes-data-path))

(-> broken-df
    (g/limit 3)
    g/show)
; +-----------------------------------------------------------------------------------------------------------------------------------------------------------------+
; |Date;Berri 1;Br�beuf (donn�es non disponibles);C�te-Sainte-Catherine;Maisonneuve 1;Maisonneuve 2;du Parc;Pierre-Dupuy;Rachel1;St-Urbain (donn�es non disponibles)|
; +-----------------------------------------------------------------------------------------------------------------------------------------------------------------+
; |01/01/2012;35;;0;38;51;26;10;16;                                                                                                                                 |
; |02/01/2012;83;;1;68;153;53;6;43;                                                                                                                                 |
; |03/01/2012;135;;2;104;248;89;3;58;                                                                                                                               |
; +-----------------------------------------------------------------------------------------------------------------------------------------------------------------+
```
