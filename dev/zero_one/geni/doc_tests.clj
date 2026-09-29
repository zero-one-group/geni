(ns zero-one.geni.doc-tests
  "Generate doc tests with numerical comparisons scoped to their assertions."
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [lread.test-doc-blocks :as blocks]
            [rewrite-clj.zip :as z]))

(defn- adapt-assertions [source]
  (loop [loc (z/of-string source)]
    (if (z/end? loc)
      (str "(require 'zero-one.geni.doc-assertions)\n" (z/root-string loc))
      (let [assertion? (and (= :list (z/tag loc))
                            (= 'clojure.test/is (some-> loc z/down z/sexpr)))
            expr (when assertion? (-> loc z/down z/right))
            equality? (and expr (= :list (z/tag expr))
                           (= '= (some-> expr z/down z/sexpr)))]
        (recur
         (z/next
          (if equality?
            (let [actual (last (z/sexpr expr))
                  stdout? (and (seq? actual)
                               (= 'clojure.string/split-lines (first actual)))]
              (-> expr z/down
                  (z/replace (if stdout?
                               'zero-one.geni.doc-assertions/stdout=
                               'zero-one.geni.doc-assertions/value=))
                  z/up z/up))
            loc)))))))

(defn generate [{:keys [target-root] :or {target-root "target"} :as opts}]
  (blocks/gen-tests opts)
  (doseq [file (file-seq (io/file target-root "test-doc-blocks/test"))
          :when (string/ends-with? (.getName file) "_test.clj")]
    (spit file (adapt-assertions (slurp file)))))
