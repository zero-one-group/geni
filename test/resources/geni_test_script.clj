(-> (rdd/text-file "test/resources/rdd.txt" 1)
    (rdd/map count)
    (rdd/reduce +)
    println)

(def training-set
  (future
    (g/table->dataset
      @spark
      [[0 "a b c d e spark"  1.0]
       [1 "b d"              0.0]
       [2 "spark f g h"      1.0]
       [3 "hadoop mapreduce" 0.0]]
      [:id :text :label])))

(g/show-vertical @training-set)
