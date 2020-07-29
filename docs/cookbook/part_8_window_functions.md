# CB8: Window Functions

This part is based on [Databricks' post on window functions](https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html). Window functions allow us to perform grouped operations such as aggregations, ranking and lagging without having to do a separate group-by and join. We are going to use a synthetic dataset:

```clojure
(def product-revenue
  (g/table->dataset
    [["Thin"       "Cell phone" 6000]
     ["Normal"     "Tablet"     1500]
     ["Mini"       "Tablet"     5500]
     ["Ultra Thin" "Cell phone" 5000]
     ["Very Thin"  "Cell phone" 6000]
     ["Big"        "Tablet"     2500]
     ["Bendable"   "Cell phone" 3000]
     ["Foldable"   "Cell phone" 3000]
     ["Pro"        "Tablet"     4500]
     ["Pro2"       "Tablet"     6500]]
    [:product :category :revenue]))

(g/print-schema product-revenue)
; root
;  |-- product: string (nullable = true)
;  |-- category: string (nullable = true)
;  |-- revenue: long (nullable = true)
```

## 8.1 The Best and Second Best in Every Category

The easiest way to define a windowed column is to use `g/windowed`. The function accepts a map that expects `:window-col` and optionally `:partition-by`, `:order-by`, `:range-between` and `:rows-between`. Consider the following example:

```clojure
(def rank-by-category
  (g/windowed
    {:window-col   (g/dense-rank)
     :partition-by :category
     :order-by     (g/desc :revenue)}))

(-> product-revenue
    (g/with-column :rank-by-category rank-by-category)
    (g/filter (g/< :rank-by-category 3))
    g/show)
; +----------+----------+-------+----------------+
; |product   |category  |revenue|rank-by-category|
; +----------+----------+-------+----------------+
; |Thin      |Cell phone|6000   |1               |
; |Very Thin |Cell phone|6000   |1               |
; |Ultra Thin|Cell phone|5000   |2               |
; |Pro2      |Tablet    |6500   |1               |
; |Mini      |Tablet    |5500   |2               |
; +----------+----------+-------+----------------+
```

The column `rank-by-category` essentially specifies the rank of the revenue in descending order grouped-by the categories. The rank starts from one, and taking the best and second best means filtering for `:rank-by-category` less than 3. Note that ties are included here.

## 8.2 Revenue Differences to the Best in Every Category

To achieve this, we can compose two windowed operations:

```clojure
(def max-by-category
  (g/windowed
    {:window-col   (g/max :revenue)
     :partition-by :category}))

(-> product-revenue
    (g/with-column :max-by-category max-by-category)
    (g/with-column :revenue-diff (g/- :max-by-category :revenue))
    (g/order-by :category (g/desc :revenue))
    g/show)
; +----------+----------+-------+---------------+------------+
; |product   |category  |revenue|max-by-category|revenue-diff|
; +----------+----------+-------+---------------+------------+
; |Thin      |Cell phone|6000   |6000           |0           |
; |Very Thin |Cell phone|6000   |6000           |0           |
; |Ultra Thin|Cell phone|5000   |6000           |1000        |
; |Bendable  |Cell phone|3000   |6000           |3000        |
; |Foldable  |Cell phone|3000   |6000           |3000        |
; |Pro2      |Tablet    |6500   |6500           |0           |
; |Mini      |Tablet    |5500   |6500           |1000        |
; |Pro       |Tablet    |4500   |6500           |2000        |
; |Big       |Tablet    |2500   |6500           |4000        |
; |Normal    |Tablet    |1500   |6500           |5000        |
; +----------+----------+-------+---------------+------------+
```

## 8.3 Revenue Differences to the Next Best in Every Category

Similar idea as the previous one, but instead of aggregating with `g/max`, we use [the analytic function](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-functions-windows.html) `g/lag` with an offset of one row:

```clojure
(def next-best-by-category
  (g/windowed
    {:window-col   (g/lag :revenue 1)
     :partition-by :category
     :order-by     (g/desc :revenue)}))

(-> product-revenue
    (g/with-column :next-best-by-category next-best-by-category)
    (g/with-column :revenue-diff (g/- :next-best-by-category :revenue))
    g/show)
; +----------+----------+-------+---------------------+------------+
; |product   |category  |revenue|next-best-by-category|revenue-diff|
; +----------+----------+-------+---------------------+------------+
; |Thin      |Cell phone|6000   |null                 |null        |
; |Very Thin |Cell phone|6000   |6000                 |0           |
; |Ultra Thin|Cell phone|5000   |6000                 |1000        |
; |Bendable  |Cell phone|3000   |5000                 |2000        |
; |Foldable  |Cell phone|3000   |3000                 |0           |
; |Pro2      |Tablet    |6500   |null                 |null        |
; |Mini      |Tablet    |5500   |6500                 |1000        |
; |Pro       |Tablet    |4500   |5500                 |1000        |
; |Big       |Tablet    |2500   |4500                 |2000        |
; |Normal    |Tablet    |1500   |2500                 |1000        |
; +----------+----------+-------+---------------------+------------+
```

## 8.4 Underperformance by One Sigma in Every Category

Suppose we would like to identify all products that are underperforming by one standard deviation from all the other products in the group. We can compose windowed columns in a single form:

```clojure
(def mean-by-category
  (g/windowed {:window-col (g/mean :revenue) :partition-by :category}))

(def std-by-category
  (g/windowed {:window-col (g/stddev :revenue) :partition-by :category}))

(-> product-revenue
    (g/with-column
      :z-stat-by-category
      (g// (g/- :revenue mean-by-category) std-by-category))
    (g/filter (g/< :z-stat-by-category -1))
    g/show)
; +--------+----------+-------+-------------------+
; |product |category  |revenue|z-stat-by-category |
; +--------+----------+-------+-------------------+
; |Bendable|Cell phone|3000   |-1.0550087574332592|
; |Foldable|Cell phone|3000   |-1.0550087574332592|
; |Normal  |Tablet    |1500   |-1.2538313376430714|
; +--------+----------+-------+-------------------+
```
