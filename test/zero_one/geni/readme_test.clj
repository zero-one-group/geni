(ns zero-one.geni.readme-test
  "Checks the README's deps.edn snippets against Geni's own deps.edn and
  build.clj, so that the Spark setups it shows stay the ones the tests run
  with, and its version stays the one being released."
  (:require
   [clojure.edn :as edn]
   [clojure.test :refer [deftest is testing]]))

(defn- edn-blocks [path]
  (->> (re-seq #"(?ms)^```edn\n(.*?)^```" (slurp path))
       (map (comp edn/read-string second))))

(defn- build-version []
  (second (re-find #"\(def version \"([^\"]+)\"\)" (slurp "build.clj"))))

(deftest readme-spark-setups-test
  (let [deps-edn (edn/read-string (slurp "deps.edn"))
        setups   (apply merge (keep :aliases (edn-blocks "README.md")))]
    (is (= #{:spark :spark-3.5-2.13 :spark-4} (set (keys setups))))
    (doseq [[alias setup] setups]
      (testing alias
        (is (= (get-in deps-edn [:aliases alias :extra-deps]) (:extra-deps setup)))
        (is (= (get-in deps-edn [:aliases alias :jvm-opts]) (:jvm-opts setup)))))))

(deftest readme-version-test
  (is (= [(build-version)]
         (keep #(get-in % [:deps 'zero.one/geni :mvn/version]) (edn-blocks "README.md")))))
