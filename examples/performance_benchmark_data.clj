(ns examples.performance-benchmark-data
  (:require
    [zero-one.geni.core :as g]))

(def data-path "/data/performance-benchmark-data")

(def skeleton-df
  (g/cache (g/table->dataset (repeat (int 2e6) [1]) [:dummy])))

(def transaction-id-col
  (g/concat (g/str (g/random-int))
            (g/lit "-")
            (g/str (g/random-int))
            (g/lit "-")
            (g/str (g/random-int))))

(def date-col
  (g/concat :year (g/lit "-") :month (g/lit "-") :day))

(def max-days {1  31
               2  28
               3  31
               4  30
               5  31
               6  30
               7  31
               8  31
               9  30
               10 31
               11 30
               12 30})

(doall
  (for [month (range 1 13)]
    (-> skeleton-df
        (g/with-column :transaction-id transaction-id-col)
        (g/with-column :member-id (g/int (g/rexp 5e-6)))
        (g/with-column :quantity (g/int (g/inc (g/rexp))))
        (g/with-column :unit-price (g/pow 2 (g/random-int 16 20)))
        (g/with-column :style-id (g/int (g/rexp 1e-2)))
        (g/with-column :brand-id (g/int (g/rexp 1e-2)))
        (g/with-column :year 2019)
        (g/with-column :month month)
        (g/with-column :day (g/random-int 1 (inc (max-days month))))
        (g/with-column :date (g/to-date date-col))
        (g/drop :dummy)
        (g/coalesce 1)
        (g/write-parquet! data-path {:mode "append"}))))

(comment

  (-> (g/read-parquet! data-path)
      (g/group-by :member-id)
      g/count
      (g/describe :count)
      g/show)

  true)
