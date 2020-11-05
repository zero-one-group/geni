# Collect data to the driver


So far we have used Spark functions which get executed on all nodes of the cluster by the Spark engine.
Spark has a lot of different options to operate on data, but sometimes we want to manipulate
the data differently in pure Clojure.

This requires to move the data from the Spark workers into the driver node, on which the Clojure Repl is running. This is only useful and possible, if the data is **small**, and fits on the node.

Geni offers several functions starting with `collect-` which transport the data to the driver and then into the Clojure Repl.



## Collect as Clojure data

A very common case is to access the data as Clojure maps with `collect`

```clojure

 (-> fixed-df (g/limit 2) g/collect)
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
    )
```

Alternatively we can get the data as a sequence of vectors with `collect-vals`

```clojure

(-> fixed-df (g/limit 2) g/collect-vals)
=>
(["01/01/2012" 35 nil 0 38 51 26 10 16 nil]
["02/01/2012" 83 nil 1 68 153 53 6 43 nil])


```

To access a the values of a single column, we can use `collect-col`:

```clojure
 (-> fixed-df (g/limit 2) (g/collect-col :Date))
 =>
("01/01/2012" "02/01/2012")

```

## Collect as arrow files

We can get the data into the driver as arrow files as well, by using the function `collect-to-arrow`
This has the advantage, that it can work with data larger then the heap space of the driver.

The condition is, that the **largest partition** of the data fits into the driver, as the data gets transported by partition.
To make this sure, we can repartition the data before collecting it.

The `collect-to-arrow` function needs as well to know, how many rows each arrow file should get.
This should be set as well small enough, so that each arrow files fits in heap space.

The function will then create various arrow files, each having `chunk-size` rows. (except the last one, which is smaller)

We need to specify as well the target directory, where the files get written to.

The function returns a sequence of file names created.

```clojure
(-> fixed-df 
  (g/repartition 20)      ;; split data in 20 partitions of equal size
  (g/collect-to-arrow 100 "/tmp"))
=>  
["/tmp/geni12331590604347994819.ipc" "/tmp/geni2107925719499812901.ipc" "/tmp/geni2239579196531625282.ipc" "/tmp/geni14530350610103010872.ipc"]


```

Setting the number of partitions and chunk size small enough, should allow the transfer of arbitrary large data to the driver. But it can obviously become slow, if data is big.

The files are written in the arrow-stream format, which can be processed by other software packages or with the Clojure "tech.ml.dataset" library.



## Integration with tech.ml.dataset

The very latest alpha version of tech.ml.dataset (tech.ml.dataset)[https://github.com/techascent/tech.ml.dataset]
offers a deeper integration with Geni, and allows to convert a Spark data-frame directly into a tech.ml.dataset. This happens on the driver, so the data need to fit in heap space.

 See (here)[https://github.com/techascent/tech.ml.dataset/blob/43f411d224a50057ae7d8817d89eda3611b33115/src/tech/v3/libs/spark.clj#L191]

for details.
