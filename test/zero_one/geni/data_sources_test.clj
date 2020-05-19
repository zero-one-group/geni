(ns zero-one.geni.data-sources-test
  (:require
    [midje.sweet :refer [fact =>]]
    [zero-one.geni.core :as g]
    [zero-one.geni.test-resources :refer [create-temp-file!
                                          melbourne-df
                                          libsvm-df
                                          spark]]))

(def write-df
  (-> melbourne-df (g/select "Method" "Type") (g/limit 5)))

(fact "Can read and write csv"
  (let [temp-file (.toString (create-temp-file! ".csv"))
        read-df  (do (g/write-csv! write-df temp-file)
                     (g/read-csv! spark temp-file))]
    (g/collect write-df) => (g/collect read-df)))

(fact "Can read and write parquet"
  (let [temp-file (.toString (create-temp-file! ".parquet"))
        read-df  (do (g/write-parquet! write-df temp-file)
                     (g/read-parquet! spark temp-file))]
    (g/collect write-df) => (g/collect read-df)))

(fact "Can read and write libsvm"
  (let [temp-file (.toString (create-temp-file! ".libsvm"))
        read-df  (do (g/write-libsvm! libsvm-df temp-file)
                     (g/read-libsvm! spark temp-file))]
    (g/collect libsvm-df) => (g/collect read-df)))

(fact "Can read and write json"
  (let [temp-file (.toString (create-temp-file! ".json"))
        read-df  (do (g/write-json! write-df temp-file)
                     (g/read-json! spark temp-file))]
    (g/collect write-df) => (g/collect read-df)))

;(fact "Can append json and parquet"
  ;(let [temp-file (.toString (create-temp-file! ".json"))
        ;read-df  (do (g/write-json! write-df temp-file {:save-mode :append})
                     ;(g/write-json! write-df temp-file {:save-mode :append})
                     ;(g/read-json! spark temp-file))]
    ;(g/count read-df) => (* 2 (g/count write-df))))
