# CB-12: Customer Segmentation with NMF

In this part, we look into the use of [non-negative matrix factorisation](https://www.nature.com/articles/44565) for customer segmentation. See [this blog post](https://medium.com/@zeroonegroup/customer-segmentation-taking-a-page-out-of-the-computer-vision-book-af02155ccf53) for context.

We will be using the [Online Retail dataset](https://archive.ics.uci.edu/dataset/352/online+retail) from the UCI Machine Learning Repository: a year of transactions from a UK-based online shop. The repo of Databricks' [Spark: The Definitive Guide](https://github.com/databricks/Spark-The-Definitive-Guide) has a copy of it as a CSV file. As in every part, we start with Geni and the `download-data!` function from [part 1](part_01_reading_and_writing_datasets.md), and this time Geni's ML namespace too:

```clojure
(require '[clojure.java.io :as io])
(require '[zero-one.geni.core :as g])
(require '[zero-one.geni.ml :as ml])

(defn download-data! [source-url target-path]
  (if (.exists (io/file target-path))
    :already-exists
    (do
      (io/make-parents target-path)
      (with-open [in (io/input-stream source-url)]
        (io/copy in (io/file target-path)))
      :downloaded)))
```

Then we download the data and load it:

```clojure
(def invoices-data-url
  "https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/4ba5601eb9b9aed1d01ab79775e3af228216ff6f/data/retail-data/all/online-retail-dataset.csv")

(def invoices-data-path "data/cookbook/online-retail.csv")

(download-data! invoices-data-url invoices-data-path)

(def invoices
  (g/read-csv! invoices-data-path {:kebab-columns true}))

(g/print-schema invoices)
;; =stdout=>
; root
;  |-- invoice-no: string (nullable = true)
;  |-- stock-code: string (nullable = true)
;  |-- description: string (nullable = true)
;  |-- quantity: integer (nullable = true)
;  |-- invoice-date: string (nullable = true)
;  |-- unit-price: double (nullable = true)
;  |-- customer-id: integer (nullable = true)
;  |-- country: string (nullable = true)

(g/count invoices)
;; => 541909

(-> invoices (g/limit 2) g/show-vertical)
; -RECORD 0------------------------------------------
;  invoice-no   | 536365
;  stock-code   | 85123A
;  description  | WHITE HANGING HEART T-LIGHT HOLDER
;  quantity     | 6
;  invoice-date | 12/1/2010 8:26
;  unit-price   | 2.55
;  customer-id  | 17850
;  country      | United Kingdom
; -RECORD 1------------------------------------------
;  invoice-no   | 536365
;  stock-code   | 71053
;  description  | WHITE METAL LANTERN
;  quantity     | 6
;  invoice-date | 12/1/2010 8:26
;  unit-price   | 3.39
;  customer-id  | 17850
;  country      | United Kingdom
```

Every row of the dataset is a transaction with the product and customer details. In collaborative-filtering settings, we typically have many users that consume many items, and each item is typically consumed by multiple users. Recommendations are done based on the common items that specific users selected and liked. The natural extension of that, in this case, would be to recommend stock codes to each customer ID based on their spending.

However, we are going to do something different this time. We represent each product by the words in its description, and call each word a 'descriptor'. By doing this, a “white hanging heart t-light holder” shares a commonality with a “white metal lantern”, because both share the word “white”, instead of having them represented by two completely different stock codes. Next, we train a non-negative matrix factorisation (NMF) model on each customer ID’s spending and their spending on each descriptor. In essence, we decompose a matrix of #-of-customers by #-of-descriptors into an individual shopping map distinct to each customer ID and a set of canonical shopping patterns shared by every customer ID.


## 12.1 Exploding Sentences into Words

To extract the descriptors of each product, we make use of [Spark’s Tokenizer](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/ml/feature/Tokenizer.html) to convert a description phrase into an array of words. However, the resulting words are filled with punctuations and irrelevant words such as “of” and “the”. Therefore, we use [Spark’s StopWordsRemover](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/ml/feature/StopWordsRemover.html), remove all punctuations and remove all resulting descriptors with less than three characters:

```clojure
(def descriptors
  (-> invoices
      (g/remove (g/null? :description))
      (ml/transform
        (ml/tokeniser {:input-col  :description
                       :output-col :descriptors}))
      (ml/transform
        (ml/stop-words-remover {:input-col  :descriptors
                                :output-col :cleaned-descriptors}))
      (g/with-column :descriptor (g/explode :cleaned-descriptors))
      (g/with-column :descriptor (g/regexp-replace :descriptor
                                                   (g/lit "[^a-zA-Z'']")
                                                   (g/lit "")))
      (g/remove (g/< (g/length :descriptor) 3))
      g/cache))

(-> descriptors
    (g/group-by :descriptor)
    (g/agg {:total-spend (g/int (g/sum (g/* :unit-price :quantity)))})
    (g/sort (g/desc :total-spend))
    (g/limit 5)
    g/show)
;; =stdout=>
; +----------+-----------+
; |descriptor|total-spend|
; +----------+-----------+
; |set       |1132690    |
; |bag       |1081737    |
; |red       |875223     |
; |retrospot |714970     |
; |heart     |706751     |
; +----------+-----------+

(-> descriptors (g/select :descriptor) g/distinct g/count)
;; => 2213
```

Notice that we cached the `descriptor` dataset as it will be used as an intermediate result, and we would not want to carry out the expensive explode operation multiple times. We end up with 2213 unique descriptors, with 'set', 'bag' and 'red' being the descriptors with the highest sales.

## 12.2 Non-Negative Matrix Factorisation

Next, to measure the association of the descriptors and the customers, we use spending. However, since money variables are typically heavily skewed on the right tail, we use log-plus-one of spending instead. Together with some cleaning measures to remove the odd transactions with negative values:

```clojure
(def log-spending
  (-> descriptors
      (g/remove (g/||
                  (g/null? :customer-id)
                  (g/< :unit-price 0.01)
                  (g/< :quantity 1)))
      (g/group-by :customer-id :descriptor)
      (g/agg {:log-spend (g/log1p (g/sum (g/* :unit-price :quantity)))})
      (g/order-by (g/desc :log-spend))))

(-> log-spending (g/describe :log-spend) g/show)
;; =stdout=>
; +-------+-------------------+
; |summary|log-spend          |
; +-------+-------------------+
; |count  |495068             |
; |mean   |3.100739905621917  |
; |stddev |1.2699691878489578 |
; |min    |0.09531017980432487|
; |max    |12.034516532838857 |
; +-------+-------------------+
```

Notice that log-spending is still heavily skewed to the right tail, but it will do for the purposes of this example.

Spark ML makes it very easy for us to train NMF models using the [ALS model](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/ml/recommendation/ALS.html). Since ALS expects the “item-id” to be integers, we need to convert the descriptors into descriptor IDs. This is also straightforward to do in Spark by using the [StringIndexer](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/ml/feature/StringIndexer.html) and putting them together in a [Pipeline](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/ml/Pipeline.html):

```clojure
(def nmf-pipeline
  (ml/pipeline
    (ml/string-indexer {:input-col  :descriptor
                        :output-col :descriptor-id})
    (ml/als {:max-iter    100
             :reg-param   0.01
             :rank        8
             :nonnegative true
             :user-col    :customer-id
             :item-col    :descriptor-id
             :rating-col  :log-spend})))
```

With 100 iterations, ALS needs a checkpoint directory. It checkpoints its intermediate results every 10 iterations when the session has one, and without one, the lineage of its datasets grows until the stack overflows. Geni's default session has no checkpoint directory, so the code sets one before fitting:

```clojure
(g/create-spark-session {:checkpoint-dir "data/cookbook/checkpoint"})

(def nmf-pipeline-model
  (ml/fit log-spending nmf-pipeline))
```

## 12.3 Linking Segments with Members and Descriptors

To extract the shared patterns and individual maps, we need to reverse the string indexer using [IndexToString](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/ml/feature/IndexToString.html) and access the user factors and item factors field in [ALSModel](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/ml/recommendation/ALSModel.html):

```clojure
(def id->descriptor
  (ml/index-to-string
    {:input-col  :id
     :output-col :descriptor
     :labels     (ml/labels (first (ml/stages nmf-pipeline-model)))}))

(def nmf-model (last (ml/stages nmf-pipeline-model)))
```

ALSModel gives us the item factors in the form of an array of factor weights, but we are only interested in the top descriptors that relate to each shared pattern. To that end, we need to use a neat SQL trick by applying posexplode to flatten the array along with the index, so that we can mark out which shared pattern the weight belongs to. Next, we use a window function to rank the weights by each shared pattern to get only, say, the top five descriptors.

```clojure
(def shared-patterns
  (-> (ml/item-factors nmf-model)
      (ml/transform id->descriptor)
      (g/select :descriptor (g/posexplode :features))
      (g/rename-columns {:pos :pattern-id
                         :col :factor-weight})
      (g/with-column
        :pattern-rank
        (g/windowed {:window-col   (g/row-number)
                     :partition-by :pattern-id
                     :order-by     (g/desc :factor-weight)}))
      (g/filter (g/< :pattern-rank 6))
      (g/order-by :pattern-id (g/desc :factor-weight))
      (g/select :pattern-id :descriptor :factor-weight)))

(-> shared-patterns
    (g/group-by :pattern-id)
    (g/agg {:descriptors (g/array-sort (g/collect-set :descriptor))})
    (g/order-by :pattern-id)
    g/show)
;; =stdout=>
; +----------+----------------------------------------------------+
; |pattern-id|descriptors                                         |
; +----------+----------------------------------------------------+
; |0         |[bag, jumbo, red, retrospot, vintage]               |
; |1         |[bottle, crystalglass, grip, milkshake, rucksack]   |
; |2         |[bar, cake, neckl, regency, set]                    |
; |3         |[afghan, crystalglass, please, shapes, sombrero]    |
; |4         |[christmas, geometric, lazer, phone, sanskrit]      |
; |5         |[bow, fur, goldie, looking, medicine]               |
; |6         |[cardpack, hall, hanging, heart, white]             |
; |7         |[george, redblue, seventeen, sideboard, transparent]|
; +----------+----------------------------------------------------+
```

To find out the soft segments each customer belongs to, we use the same trick as before, but applied to individual pattern maps and filtering only for the top-ranked pattern:

```clojure
(def customer-segments
  (-> (ml/user-factors nmf-model)
      (g/select (g/as :id :customer-id) (g/posexplode :features))
      (g/rename-columns {:pos :pattern-id
                         :col :factor-weight})
      (g/with-column
        :customer-rank
        (g/windowed {:window-col   (g/row-number)
                     :partition-by :customer-id
                     :order-by     (g/desc :factor-weight)}))
      (g/filter (g/= :customer-rank 1))))

(-> customer-segments
    (g/group-by :pattern-id)
    (g/agg {:n-customers (g/count-distinct :customer-id)})
    (g/order-by :pattern-id)
    g/show)
;; =stdout=>
; +----------+-----------+
; |pattern-id|n-customers|
; +----------+-----------+
; |0         |359        |
; |1         |403        |
; |2         |422        |
; |3         |824        |
; |4         |772        |
; |5         |524        |
; |6         |582        |
; |7         |452        |
; +----------+-----------+
```
