# Cookbook 5: String Operations

In this part of the cookbook, we are going to use the cleaned dataset from the previous part:

```clojure
(def weather-2012
  (g/read-csv! spark "target/cookbook/weather-2012.csv" {:inferSchema "true"}))
```

## 5.1 Finding The Snowiest Months

Spark (and Geni by extension) has many string operations. To filter for rows that contains "snow" in the weather column in a case insensitive way, we use `g/lower` to make the string all lower case and `g/like` to find the "snow" pattern:

```clojure
(-> weather-2012
    (g/filter (g/like (g/lower :weather) "%snow%"))
    (g/select :weather)
    g/distinct
    g/show)
; +--------------------------+
; |weather                   |
; +--------------------------+
; |Rain,Snow                 |
; |Freezing Rain,Snow Grains |
; |Snow,Fog                  |
; |Rain Showers,Snow Showers |
; |Moderate Snow,Blowing Snow|
; |Snow Showers              |
; |Drizzle,Snow,Fog          |
; |Moderate Snow             |
; |Snow,Haze                 |
; |Rain,Snow,Fog             |
; |Snow Pellets              |
; |Rain,Snow,Ice Pellets     |
; |Snow Showers,Fog          |
; |Snow,Blowing Snow         |
; |Rain,Snow Grains          |
; |Snow,Ice Pellets          |
; |Snow                      |
; |Freezing Drizzle,Snow     |
; |Drizzle,Snow              |
; +--------------------------+
```

We verify that every distinct row contains the word "snow". To find the snowiest month, we do the usual group-by + aggregate operation to count the distinct dates of every weather:

```clojure
(-> weather-2012
    (g/filter (g/like (g/lower :weather) "%snow%"))
    (g/group-by :year :month)
    (g/agg {:n-days (g/count-distinct :day)})
    (g/order-by :year :month)
    g/show)
; +----+-----+------+
; |year|month|n-days|
; +----+-----+------+
; |2012|1    |22    |
; |2012|2    |19    |
; |2012|3    |10    |
; |2012|4    |3     |
; |2012|11   |6     |
; |2012|12   |22    |
; +----+-----+------+
```

We find that January and December are indeed the snowiest months in Montréal.

## 5.2 Putting Snowiness and Temperature Together

To determine the snowiness of a month, we compute the proportion of snowy days to the total number of days in that month. To count the number of snowy days, we count the number of distinct days **only for snowy days**. To achieve this we use `g/when` with only the then-clause, namely `(g/when (g/like (g/lower :weather) "%snow%") :day)`. This says that if the row's weather contains "snow", then it takes the value of the column `:day`, otherwise it is null.

```clojure
(-> weather-2012
    (g/group-by :year :month)
    (g/agg
      {:n-snow-days (g/count-distinct
                     (g/when (g/like (g/lower :weather) "%snow%") :day))
       :n-days      (g/count-distinct :day)
       :mean-temp   (g/mean :temp)})
    (g/order-by :year :month)
    (g/select {:year      :year
               :month     :month
               :snowiness (g/format-number (g// :n-snow-days :n-days) 2)
               :mean-temp (g/format-number :mean-temp 1)})
    g/show)
; +----+-----+---------+---------+
; |year|month|snowiness|mean-temp|
; +----+-----+---------+---------+
; |2012|1    |0.71     |-7.4     |
; |2012|2    |0.66     |-4.2     |
; |2012|3    |0.32     |3.1      |
; |2012|4    |0.10     |7.0      |
; |2012|5    |0.00     |16.2     |
; |2012|6    |0.00     |20.1     |
; |2012|7    |0.00     |22.8     |
; |2012|8    |0.00     |22.3     |
; |2012|9    |0.00     |16.5     |
; |2012|10   |0.00     |11.0     |
; |2012|11   |0.20     |0.9      |
; |2012|12   |0.71     |-3.3     |
; +----+-----+---------+---------+
```

Notice that we used `g/format-number` to specify the number of decimal places to show.

## 5.3 Finding Temperatures of Common Weather Descriptions

In each row, the `:weather` value is a comma-separated weather description. To separate the individual weather descriptions, we can use `(g/split :weather ",")` to get a column of list of strings. We further invoke `g/explode` on this column, which is analogous to the flat-map operation or Clojure's `mapcat`:

```clojure
(-> weather-2012
    (g/with-column :weather-description (g/explode (g/split :weather ",")))
    (g/group-by :weather-description)
    (g/agg {:mean-temp (g/mean :temp)
            :n-days    (g/count-distinct :year :month :day)})
    (g/order-by (g/desc :mean-temp))
    (g/select {:weather-description :weather-description
               :mean-temp (g/format-number :mean-temp 1)
               :n-days :n-days})
    (g/show {:num-rows 25}))
; +---------------------+---------+------+
; |weather-description  |mean-temp|n-days|
; +---------------------+---------+------+
; |Thunderstorms        |20.2     |14    |
; |Moderate Rain Showers|19.6     |1     |
; |Rain Showers         |14.2     |99    |
; |Mainly Clear         |12.6     |288   |
; |Heavy Rain Showers   |10.9     |1     |
; |Mostly Cloudy        |10.6     |315   |
; |Rain                 |9.0      |86    |
; |Cloudy               |8.0      |285   |
; |Drizzle              |6.9      |38    |
; |Clear                |6.8      |216   |
; |Fog                  |5.0      |77    |
; |Moderate Rain        |1.7      |1     |
; |Snow Pellets         |0.7      |1     |
; |Ice Pellets          |-0.7     |6     |
; |Haze                 |-1.2     |7     |
; |Snow Grains          |-1.6     |2     |
; |Freezing Rain        |-3.7     |7     |
; |Snow Showers         |-3.8     |25    |
; |Snow                 |-4.1     |71    |
; |Freezing Drizzle     |-4.7     |12    |
; |Blowing Snow         |-5.4     |5     |
; |Moderate Snow        |-5.5     |2     |
; |Freezing Fog         |-7.6     |3     |
; +---------------------+---------+------+
```
