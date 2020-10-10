(require '[zero-one.geni.core :as g])

(def data-path "data/performance-benchmark-data")

(def skeleton-df
  (g/cache (g/table->dataset (repeat (int 2e6) [1]) [:dummy])))

(defn transaction-id-col []
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
       (g/select
        {:trx-id    (transaction-id-col)
         :member-id (g/int (g/rexp 5e-6))
         :quantity  (g/int (g/inc (g/rexp)))
         :price     (g/pow 2 (g/random-int 16 20))
         :style-id  (g/int (g/rexp 1e-2))
         :brand-id  (g/int (g/rexp 1e-2))
         :year      2019
         :month     month
         :day       (g/random-int 1 (inc (max-days month)))})
       (g/with-column :date (g/to-date date-col))
       (g/coalesce 1)
       (g/write-parquet! data-path {:mode "append"}))))
