(ns zero-one.geni.tech-ml-test
  (:require
   [midje.sweet :refer [facts fact throws =>]]
   [zero-one.geni.core :as g]
   [zero-one.geni.test-resources :refer [create-temp-file! melbourne-df]]))

(def dummy-df
  (-> (melbourne-df) (g/select :Method) (g/limit 5)))

(fact "On rand-nth" :slow
  (g/rand-nth dummy-df) => map?
  (g/rand-nth (melbourne-df)) => map?
  (g/rand-nth (g/limit (melbourne-df) 1)) => map?
  (g/rand-nth (g/limit (melbourne-df) 2)) => map?)

(fact "On assoc and dissoc"
  (-> dummy-df
      (g/assoc :always-ten 10)
      (g/collect-col :always-ten)
      set) => #{10}
  (-> dummy-df (g/assoc :always-ten 10 :always-two)) => (throws  Exception)
  (-> dummy-df
      (g/assoc :always-ten 10
               :always-two 2)
      g/columns) => [:Method :always-ten :always-two]
  (-> dummy-df
      (g/assoc :always-ten 10)
      (g/dissoc :Method)
      g/columns) => [:always-ten]
  (-> dummy-df
      (g/assoc :always-ten 10)
      (g/dissoc :Method
                :always-ten)
      g/columns) => [])

(facts "On ->dataset" :slow
  (fact "->dataset with sequence of maps"
    (g/collect (g/->dataset [{:a 1 :b 2} {:a 2 :c 3}]))
    => [{:a 1 :b 2 :c nil} {:a 2 :b nil :c 3}])

  (fact "->dataset with viable file paths"
    (doall
     (for [[ext write-fn!] {:avro    g/write-avro!
                            :csv     g/write-csv!
                            :json    g/write-json!
                            :parquet g/write-parquet!}]
       (let [temp-file (.toString (create-temp-file! (str "." (name ext))))]
         (write-fn! dummy-df temp-file {:mode "overwrite"})
         (g/collect (g/->dataset temp-file)) => (g/collect dummy-df)))))

  (fact "->dataset with unviable file path"
    (g/->dataset "test/resources/sample_kmeans_data.txt") => (throws Exception))

  (fact "->dataset with options"
    (-> [{:a 1 :b 2} {:a 2 :c 3}]
        (g/->dataset {})
        g/collect) => [{:a 1 :b 2 :c nil} {:a 2 :b nil :c 3}]
    (-> "test/resources/melbourne_housing_snapshot.parquet"
        (g/->dataset {})
        g/count) => 13580
    (-> "test/resources/melbourne_housing_snapshot.parquet"
        (g/->dataset {:n-records 5})
        g/count) => 5
    (-> "test/resources/melbourne_housing_snapshot.parquet"
        (g/->dataset {:column-whitelist [:Price "SellerG"]})
        g/columns) => [:Price :SellerG]))

(fact "On name-value-seq->dataset"
  (g/collect (g/name-value-seq->dataset {:age [1 2 3 4 5] :name ["a" "b" "c" "d" "e"]}))
  => [{:age 1 :name "a"}
      {:age 2 :name "b"}
      {:age 3 :name "c"}
      {:age 4 :name "d"}
      {:age 5 :name "e"}])
