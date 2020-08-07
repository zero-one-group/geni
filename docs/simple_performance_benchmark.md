# A Simple Performance Benchmark

The Geni project was initiated by [Zero One Group's](https://zero-one-group.com/) data team in mid-2020 partly due to our frustrations with Pandas' poor single-threaded performance. We could have gone the PySpark way, but since the rest of the team had started using Clojure, we wanted have a crack at using Clojure our data jobs.

The following piece does not attempt to present a fair, rigorous performance benchmark results. Instead, we would like to illustrate the kinds of speedups that were up for grasp for our team and our specific use case. Therefore, by all means, the results should be taken with a grain salt!

## Dummy Retail Data

In mid-2020, we worked on a customer segmentation project for one of Indonesia's retail giants. We were working with more than 20 million transactions and 4 million customers. We simulate a reasonably representative dummy data, and the script can be found [here](../examples/performance_benchmark_data.clj).

## A Group-By + Aggregate Operation

The dummy data contains exactly 24 million transactions and approximately one million customers. We simply would like to know, for each customer:

- how much they spent;
- their average basket size;
- their average spend per transaction;
- the number of transactions;
- the number of visits;
- the number of different brands they purchased; and
- the number of different styles they purchased.

In Geni, we increase the JVM maximum heap size to 16GB and enabled Spark 3's adaptive query execution and dynamic coalescing of partitions using the following config:

```clojure
{:configs {:spark.sql.adaptive.enabled "true"
           :spark.sql.adaptive.coalescePartitions.enabled "true"}}
```

The actual aggregation is as follows:

```clojure
(-> dataframe
    (g/with-column :sales (g/* :price :quantity))
    (g/group-by :member-id)
    (g/agg {:total-spend     (g/sum :sales)
            :avg-basket-size (g/mean :sales)
            :avg-price       (g/mean :price)
            :n-transactions  (g/count "*")
            :n-visits        (g/count-distinct :date)
            :n-brands        (g/count-distinct :brand-id)
            :n-styles        (g/count-distinct :style-id)})
```

and in Pandas:

```python
(transactions
    .groupby('member-id')
    .apply(lambda grouped: pd.Series({
        'total-spend': grouped['sales'].sum(),
        'avg-basket-size': grouped['sales'].mean(),
        'avg-price': grouped['price'].mean(),
        'n-transactions': len(grouped),
        'n-visits': len(grouped['date'].unique()),
        'n-brands': len(grouped['brand-id'].unique()),
        'n-styles': len(grouped['style-id'].unique()),
    })))
```

## Results

The following results are obtained from a machine with a 12-core Intel(R) Core(TM) i7-5930K CPU @ 3.50GHz, 3 x 8GB of Corsair's DDR4 RAM and 512GB Samsung Electronics NVMe SSD Controller SM981/PM981.

```bash
| Runtime (s) | N=2,000,000 | xGeni | N=24,000,000 | xGeni |
| ---         | ---         | ---   | ---          | ---   |
| Pandas      | 587         | x73.4 | 1,132        | x29.0 |
| Geni        | 8           | x1.0  | 39           | x1.0  |
```

When run on only one month of data, Geni is 73x faster than Pandas. When run on the full dataset, Geni is 29x faster than Pandas. Much of the gap is due to Pandas not using all of the available cores on the machine, which should account for, at most, 12x in performance gains.

These speedup factors are typical whenever we compare Pandas and Geni on our particular setup. Again, this is not meant to be a serious benchmark exercise, rather an illustration of what we typically see on our particular setup.
