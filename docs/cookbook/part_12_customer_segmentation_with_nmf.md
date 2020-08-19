# CB-12: Customer Segmentation with NMF

In this part, we look into the use of [non-negative matrix factorisation](https://www.nature.com/articles/44565) for customer segmentation. See [this blog post](https://medium.com/@zeroonegroup/customer-segmentation-taking-a-page-out-of-the-computer-vision-book-af02155ccf53) for context.

We will be using the Online Retail II dataset, which is [available for free on Kaggle](https://www.kaggle.com/hikne707/online-retail?select=online_retail_II.xlsx). Since the dataset is behind a sign-up wall, we assume that the two CSV files are already downloaded and placed in the `data/online_retail_ii` directory. We load the dataset as follows:

```clojure
(def invoices
  (g/read-csv! "data/online_retail_ii" {:kebab-columns true}))

(g/print-schema invoices)
; root
;  |-- invoice: string (nullable = true)
;  |-- stock-code: string (nullable = true)
;  |-- description: string (nullable = true)
;  |-- quantity: integer (nullable = true)
;  |-- invoice-date: string (nullable = true)
;  |-- price: double (nullable = true)
;  |-- customer-id: integer (nullable = true)
;  |-- country: string (nullable = true)

(g/count invoices)
; 1067371

(-> invoices (g/limit 2) g/show-vertical)
; -RECORD 0-------------------------------------------
;  invoice      | 489434
;  stock-code   | 85048
;  description  | 15CM CHRISTMAS GLASS BALL 20 LIGHTS
;  quantity     | 12
;  invoice-date | 1/12/2009 07:45
;  price        | 6.95
;  customer-id  | 13085
;  country      | United Kingdom
; -RECORD 1-------------------------------------------
;  invoice      | 489434
;  stock-code   | 79323P
;  description  | PINK CHERRY LIGHTS
;  quantity     | 12
;  invoice-date | 1/12/2009 07:45
;  price        | 6.75
;  customer-id  | 13085
;  country      | United Kingdom
```

Every row of the dataset is a transaction with the product and customer details. In collaborative-filtering settings, we typically have many users that consume many items, and each item is typically consumed by multiple users. Recommendations are done based on the common items that specific users selected and liked. The natural extension of that, in this case, would be to recommend stock codes to each customer ID based on their spending.

However, we are going to do something different this time. We represent each product by the words in its description, and call each word a 'descriptor'. By doing this, a “15cm christmas glass ball 20 lights” share a commonality with “pink cherry lights”, because both share the word “lights”, instead of having them represented by two completely different stock codes. Next, we train a non-negative matrix factorisation (NMF) model on each customer ID’s spending and their spending on each descriptor. In essence, we decompose a matrix of #-of-customers by #-of-descriptors into an individual shopping map distinct to each customer ID and a set of canonical shopping patterns shared by every customer ID.


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
    (g/agg {:total-spend (g/sum (g/* :price :quantity))})
    (g/sort (g/desc :total-spend))
    (g/limit 5)
    g/show)
; +----------+-----------+
; |descriptor|total-spend|
; +----------+-----------+
; |set       |2089125    |
; |bag       |1912097    |
; |red       |1834692    |
; |heart     |1465429    |
; |vintage   |1179526    |
; |retrospot |1166847    |
; |white     |1155863    |
; |pink      |1009384    |
; |jumbo     |984806     |
; |design    |917394     |
; +----------+-----------+

(-> descriptors (g/select :descriptor) g/distinct g/count)
=> 2605
```

Notice that we cached the `descriptor` dataset as it will be used as an intermediate result, and we would not want to carry out the expensive explode operation multiple times. We end up with 2605 unique descriptors with 'set', 'bag' and 'red' being the descriptors with the highest sales.

## 12.2 Non-Negative Matrix Factorisation

Next, to measure the association of the descriptors and the customers, we use spending. However, since money variables are typically heavily skewed on the right tail, we use log-plus-one of spending instead. Together with some cleaning measures to remove the odd transactions with negative values:

```clojure
(def log-spending
  (-> descriptors
      (g/remove (g/||
                  (g/null? :customer-id)
                  (g/< :price 0.01)
                  (g/< :quantity 1)))
      (g/group-by :customer-id :descriptor)
      (g/agg {:log-spend (g/log1p (g/sum (g/* :price :quantity)))})
      (g/order-by (g/desc :log-spend))))

(-> log-spending (g/describe :log-spend) g/show)
; +-------+--------------------+
; |summary|log-spend           |
; +-------+--------------------+
; |count  |837985              |
; |mean   |3.173295903226327   |
; |stddev |1.3183533551300999  |
; |min    |0.058268908123975775|
; |max    |12.034516532838857  |
; +-------+--------------------+
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
; +----------+----------------------------------------------------------+
; |pattern-id|descriptors                                               |
; +----------+----------------------------------------------------------+
; |0         |[heart, holder, jun, peter, tlight]                       |
; |1         |[bar, draw, garld, seventeen, sideboard]                  |
; |2         |[coathangers, jun, peter, pinkblack, rucksack]            |
; |3         |[bag, jumbo, lunch, red, retrospot]                       |
; |4         |[retrodisc, rnd, scissor, sculpted, shapes]               |
; |5         |[afghan, capiz, lazer, mugcoasterlavender, yellowblue]    |
; |6         |[cake, metal, sign, stand, time]                          |
; |7         |[mintivory, necklturquois, pinkamethystgold, regency, set]|
; +----------+----------------------------------------------------------+
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
; +----------+-----------+
; |pattern-id|n-customers|
; +----------+-----------+
; |0         |760        |
; |1         |1095       |
; |2         |379        |
; |3         |444        |
; |4         |1544       |
; |5         |756        |
; |6         |426        |
; |7         |474        |
; +----------+-----------+
```
