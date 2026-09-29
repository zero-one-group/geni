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
