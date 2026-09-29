(ns zero-one.geni.tech-ml-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [zero-one.geni.core :as g]
   [zero-one.geni.test-resources :refer [create-temp-file! melbourne-df]]))

(def dummy-df
  (-> (melbourne-df) (g/select :Method) (g/limit 5)))

(deftest ^:slow rand-nth-test
  (is (map? (g/rand-nth dummy-df)))
  (is (map? (g/rand-nth (melbourne-df))))
  (is (map? (g/rand-nth (g/limit (melbourne-df) 1))))
  (is (map? (g/rand-nth (g/limit (melbourne-df) 2)))))

(deftest assoc-and-dissoc-test
  (is (= #{10}
         (-> dummy-df
             (g/assoc :always-ten 10)
             (g/collect-col :always-ten)
             set)))
  (is (thrown? Exception (-> dummy-df (g/assoc :always-ten 10 :always-two))))
  (is (= [:Method :always-ten :always-two]
         (-> dummy-df
             (g/assoc :always-ten 10
                      :always-two 2)
             g/columns)))
  (is (= [:always-ten]
         (-> dummy-df
             (g/assoc :always-ten 10)
             (g/dissoc :Method)
             g/columns)))
  (is (= []
         (-> dummy-df
             (g/assoc :always-ten 10)
             (g/dissoc :Method
                       :always-ten)
             g/columns))))

(deftest ^:slow dataset-test
  (testing "->dataset with sequence of maps"
    (is (= [{:a 1 :b 2 :c nil} {:a 2 :b nil :c 3}] (g/collect (g/->dataset [{:a 1 :b 2} {:a 2 :c 3}])))))

  (testing "->dataset with viable file paths"
    (doall
     (for [[ext write-fn!] {:avro    g/write-avro!
                            :csv     g/write-csv!
                            :json    g/write-json!
                            :parquet g/write-parquet!}]
       (let [temp-file (.toString (create-temp-file! (str "." (name ext))))]
         (write-fn! dummy-df temp-file {:mode "overwrite"})
         (is (= (g/collect dummy-df) (g/collect (g/->dataset temp-file))))))))

  (testing "->dataset with unviable file path"
    (is (thrown? Exception (g/->dataset "test/resources/sample_kmeans_data.txt"))))

  (testing "->dataset with options"
    (is (= [{:a 1 :b 2 :c nil} {:a 2 :b nil :c 3}]
           (-> [{:a 1 :b 2} {:a 2 :c 3}]
               (g/->dataset {})
               g/collect)))
    (is (= 13580
           (-> "test/resources/melbourne_housing_snapshot.parquet"
               (g/->dataset {})
               g/count)))
    (is (= 5
           (-> "test/resources/melbourne_housing_snapshot.parquet"
               (g/->dataset {:n-records 5})
               g/count)))
    (is (= [:Price :SellerG]
           (-> "test/resources/melbourne_housing_snapshot.parquet"
               (g/->dataset {:column-whitelist [:Price "SellerG"]})
               g/columns)))))

(deftest name-value-seq-dataset-test
  (is (= [{:age 1 :name "a"}
          {:age 2 :name "b"}
          {:age 3 :name "c"}
          {:age 4 :name "d"}
          {:age 5 :name "e"}]
         (g/collect (g/name-value-seq->dataset {:age [1 2 3 4 5] :name ["a" "b" "c" "d" "e"]})))))
