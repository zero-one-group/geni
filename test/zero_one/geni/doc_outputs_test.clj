(ns zero-one.geni.doc-outputs-test
  (:require [clojure.string :as string]
            [clojure.test :refer [deftest is testing]]
            [zero-one.geni.doc-outputs :as outputs]))

(defn- with-doc [source f]
  (let [file (java.io.File/createTempFile "geni-doc-outputs-" ".md")]
    (try
      (spit file source)
      (f (.getPath file))
      (finally (.delete file)))))

(deftest refresh-expectations-test
  (with-doc
    (str "# Example\n\n```clojure\n(def x 3)\n```\n\nKeep this prose.\n\n"
         "```clojure\n(+ x 2)\n;; => nil\n(println x)\n;; =stdout=>\n; old\n```\n")
    (fn [path]
      (is (outputs/regen-file path))
      (let [result (slurp path)]
        (is (string/includes? result "Keep this prose."))
        (is (string/includes? result ";; => 5"))
        (is (string/includes? result ";; =stdout=>\n; 3"))
        (is (not (outputs/regen-file path)))
        (is (= result (slurp path)))))))

(deftest skipped-doc-blocks-test
  (doseq [directive ["<!-- :test-doc-blocks/skip -->"
                     "<!-- {:test-doc-blocks/skip true :test-doc-blocks/apply :all-next} -->"]]
    (with-doc (str directive "\n\n```clojure\n(throw (Exception. \"skip me\"))\n```\n"
                   "\n```clojure\n(+ 1 2)\n;; => 3\n```\n")
      (fn [path] (is (not (outputs/regen-file path))))))
  (with-doc (str "<!-- {:test-doc-blocks/skip true :test-doc-blocks/apply :all-next} -->\n"
                 "```clojure\n(throw (Exception.))\n```\n\nMore prose.\n\n"
                 "```clojure\n(throw (Exception.))\n```\n")
    (fn [path] (is (not (outputs/regen-file path))))))

(deftest failed-refresh-preserves-doc-test
  (doseq [source ["```clojure\n(+ 1 2)\n;; => nil\n(throw (Exception. \"broken\"))\n```\n"
                  "```clojure\n(+ 1 2)\n"]]
    (with-doc source
      (fn [path]
        (testing "errors preserve the original file and clean up the namespace"
          (is (thrown? Exception (outputs/regen-file path)))
          (is (= source (slurp path)))
          (is (nil? (find-ns (symbol (str "doc-outputs." (string/replace path #"[^A-Za-z0-9]+" "-")))))))))))

(deftest long-flat-values-test
  (with-doc (str "```clojure\n(range 40)\n;; => nil\n(vec (map str (range 30)))\n;; => nil\n"
                 "(map (fn [i] {:i i :squared (* i i) :cubed (* i i i)}) (range 3 6))\n;; => nil\n"
                 "(print \"\")\n;; =stdout=>\n; old\n```\n")
    (fn [path]
      (outputs/regen-file path)
      (let [lines (string/split-lines (slurp path))]
        (testing "flat collections are filled, within 80 columns"
          (is (= [";; => (0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27"
                  ";;     28 29 30 31 32 33 34 35 36 37 38 39)"]
                 (->> lines (drop-while #(not= "(range 40)" %)) rest (take 2))))
          (is (every? #(<= (count %) 80) lines)))
        (testing "nested values are still pretty-printed"
          (is (some #{";;     {:i 4, :squared 16, :cubed 64}"} lines)))
        (testing "an empty =stdout=> is dropped"
          (is (not-any? #(string/includes? % "stdout") lines)))))))
