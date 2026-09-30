(ns zero-one.geni.template-test
  "Checks the deps-new template in template/ against Geni's own deps.edn and
  build.clj, so that the projects it makes get the Spark setups that the tests
  run with, and the Geni version being released."
  (:require
   [clojure.edn :as edn]
   [clojure.test :refer [deftest is testing]]))

(def ^:private template-dir "template/resources/zero_one/geni")

(deftest template-spark-setups-test
  (let [deps-edn (edn/read-string (slurp "deps.edn"))
        template (edn/read-string (slurp (str template-dir "/build/deps.tmpl")))]
    (doseq [alias [:spark :spark-4]]
      (testing alias
        (is (= (get-in deps-edn [:aliases alias]) (get-in template [:aliases alias])))))))

(deftest template-version-test
  (is (= (second (re-find #"\(def version \"([^\"]+)\"\)" (slurp "build.clj")))
         (:geni/version (edn/read-string (slurp (str template-dir "/template.edn")))))))
