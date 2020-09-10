# A Simple Performance Benchmark

The Geni project was initiated by [Zero One Group's](https://zero-one-group.com/) data team in mid-2020 partly due to our frustrations with Pandas' poor single-threaded performance. We could have gone the PySpark way, but since the rest of the team had started using Clojure, we wanted have a crack at using Clojure for our data jobs.

The following piece does not attempt to present a fair, rigorous performance benchmark results. Instead, it is to illustrate typical speedups that were up for grasp for our team and for our specific use cases. Therefore, the results presented here should be taken with a grain of salt.

For the sake of completeness, we also include the popular Clojure library [tech.ml.dataset](https://github.com/techascent/tech.ml.dataset) (or TMD) and the popular R library [dplyr](https://dplyr.tidyverse.org/).

## Dummy Retail Data

In mid-2020, we worked on a customer segmentation project for one of Indonesia's retail giants. We were working with more than 20 million transactions and 4 million customers. We simulate a reasonably representative dummy data with Geni. The crux of the simulations is as follows:

```clojure
(-> skeleton-df
    (g/select
      {:trx-id    (transaction-id-col)
       :member-id (g/int (g/rexp 5e-6) )
       :quantity  (g/int (g/inc (g/rexp)))
       :price     (g/pow 2 (g/random-int 16 20))
       :style-id  (g/int (g/rexp 1e-2))
       :brand-id  (g/int (g/rexp 1e-2))
       :year      2019
       :month     month
       :day       (g/random-int 1 (inc (max-days month)))})
    (g/with-column :date (g/to-date date-col)))
```

The full dataset is stored in twelve partitions - one for each month of the year. The full data-simulation script can be found [here](../examples/performance_benchmark_data.clj).

## A Group-By + Aggregate Operation

The dummy data contains exactly 24 million transactions and approximately one million customers. The task is simple. We would like to know, for each customer:

- how much they spent;
- their average basket size;
- their average spend per transaction;
- the number of transactions;
- the number of visits;
- the number of different brands they purchased; and
- the number of different styles they purchased.

### Clojure: Geni

We do the following aggregation:

```clojure
(-> dataframe
    (g/group-by :member-id)
    (g/agg {:total-spend     (g/sum :sales)
            :avg-basket-size (g/mean :sales)
            :avg-price       (g/mean :price)
            :n-transactions  (g/count "*")
            :n-visits        (g/count-distinct :date)
            :n-brands        (g/count-distinct :brand-id)
            :n-styles        (g/count-distinct :style-id)})
    (g/write-parquet! "target/geni-matrix.parquet" {:mode "overwrite"}))
```

Note that, we additionally increase the JVM maximum heap size to 16GB and enabled Spark 3's adaptive query execution and dynamic coalescing of partitions using the following config:

```clojure
{:configs {:spark.sql.adaptive.enabled "true"
           :spark.sql.adaptive.coalescePartitions.enabled "true"}}
```

### Python: Pandas

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
    }))
    .to_parquet('target/pandas-matrix.parquet'))
```

The full scripts can be found [here](https://github.com/zero-one-group/geni-performance-benchmark).

### R: dplyr

```r
dataframe %>%
    mutate(sales = price * quantity) %>%
    group_by(`member-id`) %>%
    summarise(total_spend = sum(sales),
              avg_basket_size = mean(sales),
              avg_price = mean(price),
              n_transactions = n(),
              n_visits = n_distinct(date),
              n_brands = n_distinct(`brand-id`),
              n_styles = n_distinct(`style-id`)) %>%
    write_parquet("final.parquet")
```

The full script can be found [here](https://github.com/zero-one-group/geni-performance-benchmark/blob/master/r/dplyr.r).

### R: data.table

```r
dataframe[, sales := quantity * price]
result = dataframe[,
                   .(total_spend = sum(sales),
                     avg_basket_size = mean(sales),
                     avg_price = mean(price),
                     n_transactions = .N,
                     n_visits = uniqueN(date),
                     n_brands = uniqueN(`brand-id`),
                     n_styles = uniqueN(`style-id`)),
                   by = `member-id`]
write_parquet(result, "datatable.parquet")
```

The full script can be found [here](https://github.com/zero-one-group/geni-performance-benchmark/blob/master/r/datatable.r).

### Julia: DataFrames

```julia
df.sales = df.price .* df.quantity
gdf = groupby(df, :member_id)
summary = combine(gdf, :sales => sum => :total_spend,
                       :sales => mean => :avg_basket_size,
                       :price => mean => :avg_price,
                       nrow => :n_transactions,
                       :date => nunique => :n_visits,
                       :brand_id => nunique => :n_brands,
                       :style_id => nunique => :n_styles)
summary |> save("julia.feather")
```

The full script can be found [here](https://github.com/zero-one-group/geni-performance-benchmark/blob/master/julia/main.jl).

### Clojure: tech.ml.dataset

We have three TMD variants, and each one is run using the same JVM options as Geni. The first variant uses Scicloj's [tablecloth](https://github.com/scicloj/tablecloth). We found tablecloth's [dplyr](https://dplyr.tidyverse.org/)-like API to be the most straightforward and looks most like the original Geni:

```clojure
(-> dataframe
    (api/add-or-replace-column "sales" #(dfn/* (% "price") (% "quantity")))
    (api/group-by "member-id")
    (api/aggregate {:total-spend     #(dfn/sum (% "sales"))
                    :avg-basket-size #(dfn/mean (% "sales"))
                    :avg-price       #(dfn/mean (% "price"))
                    :n-transactions  api/row-count
                    :n-visits        #(count (distinct (% "date")))
                    :n-brands        #(count (distinct (% "brand-id")))
                    :n-styles        #(count (distinct (% "style-id")))}
                   {:parallel? true})
    (api/write-nippy! "target/dataset-matrix.nippy.gz"))
```

The full script can be found [here](https://github.com/zero-one-group/geni-performance-benchmark/blob/master/tablecloth/src/tablecloth/core.clj). Shout out to [Tomasz Sulej](https://github.com/tsulej) for [the performance patch](https://github.com/scicloj/tablecloth/commit/53c25e55b94e3873d51b71276a1c31ac5473ee52) and for helping us optimise the code!

After speaking to TMD's main author, [Chris Nuernberger](https://github.com/cnuernber), we found out that the tablecloth code can be further optimised. Potential bottlenecks include:

1. TMD has concatenation overheads, so that reading from a single file is faster than concatenating 12 partitions; and
2. TMD's support for Apache Parquet is poor; instead Apache Arrow should be used.

With some help from Chris, we managed to get [the second TMD variant](https://github.com/zero-one-group/geni-performance-benchmark/blob/master/dataset/src/dataset/optimised.clj), which solves problem 1. Chris wrote [the third TMD variant](https://github.com/zero-one-group/geni-performance-benchmark/blob/master/dataset/src/dataset/optimised_by_chris.clj) himself! He added support for Arrow and some custom code for further optimisation. Of course, it is the most performant variant of TMD!

Note that, at this point, the TMD comparisons are not 100% apples-to-apples, as the TMD variants have different data formats and partitions. However, it still solves the original problem, so we still include the benchmark results below!

## Results

The following results are obtained from a machine with a 12-core Intel(R) Core(TM) i7-5930K CPU @ 3.50GHz, 3 x 8GB of Corsair's DDR4 RAM and 512GB Samsung Electronics NVMe SSD Controller SM981/PM981.

| Language | Runtime (s)                          | N=2,000,000 | xGeni | N=24,000,000 | xGeni |
| --       | ---                                  | ---         | ---   | ---          | ---   |
| Python   | Pandas                               | 587         | x73.4 | 1,132        | x29.0 |
| R        | dplyr                                | 461         | x57.6 | 992          | x25.4 |
| Julia    | DataFrames (with Parquet)            | 87          | x10.9 | 868          | x22.3 |
| Clojure  | tablecloth                           | 48          | x6.0  | 151          | x3.9  |
| R        | data.table                           | 28          | x3.5  | 143          | x3.7  |
| Clojure  | tech.ml.dataset (optimised)          | 18          | x2.3  | 133          | x3.4  |
| Julia    | DataFrames (with Feather)            | 16          | x2.0  | 41           | x1.1  |
| Clojure  | tech.ml.dataset (optimised by Chris) | 9           | x1.1  | 36           | x0.9  |
| Clojure  | Geni                                 | 8           | x1.0  | 39           | x1.0  |

When run on only one month of data, Geni is 73x faster than Pandas. When run on the full dataset, Geni is 29x faster than Pandas. Much of the gap is due to Pandas not using all of the available cores on the machine, which should account for, at most, 12x in performance gains. When optimised heavily, TMD's performance is roughly the same as Geni's.

These speedup factors are typical whenever we compare Pandas and Geni. To reiterate, this is not meant to be a serious benchmark exercise, rather an illustration of what we typically see on our particular setup.
