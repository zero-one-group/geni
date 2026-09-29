(ns zero-one.geni.doc-assertions-test
  (:require [clojure.test :refer [deftest is testing]]
            [zero-one.geni.doc-assertions :as docs]))

(deftest numerical-doc-values-test
  (testing "nested ML values tolerate numerical backend differences"
    (is (docs/value= [{:p '(0.7151376213452819 0.2848623786547181)}]
                     [{:p [0.7151376213452818 0.2848623786547182]}]))
    (is (docs/value= 0.2656020220314336 0.2656020168105996))
    (is (docs/value= 0.0 1.0e-10)))
  (testing "real changes still fail"
    (doseq [[expected actual] [[0.26 0.27]
                               [0.0 1.0e-5]
                               [10000000 10000001]
                               [{:x 1.0} {:y 1.0}]
                               [[1.0 2.0] [2.0 1.0]]
                               [[1.0] [1.0 2.0]]
                               ["0.265602022" "0.265602016"]
                               [nil []]
                               [1.0 ##Inf]
                               [##NaN ##NaN]]]
      (is (not (docs/value= expected actual))))))

(deftest printed-doc-values-test
  (is (docs/stdout= ["+--+-----------+" "|id|p          |" "|17|4.626449   |"]
                    ["+--+------------+" "|id|p           |" "|17|4.6264491   |"]))
  (is (docs/stdout= ["coefficient: -7.520689871383902E-5"]
                    ["coefficient: -7.52068987138425E-5"]))
  (doseq [[expected actual] [["|17|4.626449|" "|18|4.626449|"]
                             ["|17|4.626449|" "|17|4.726449|"]
                             ["|id|p|" "|id|q|"]
                             ["+--+--+" "+--+--+--+"]
                             ["version 1.2.3" "version 1.2.4"]
                             ["count: 10000000" "count: 10000001"]
                             ["hello  world" "hello world"]]]
    (is (not (docs/stdout= [expected] [actual]))))
  (is (not (docs/stdout= ["1.0"] ["1.0" "2.0"]))))
