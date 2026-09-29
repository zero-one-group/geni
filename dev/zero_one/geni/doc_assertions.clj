(ns zero-one.geni.doc-assertions
  "Comparisons for generated doc tests only; library tests still use exact =."
  (:require [clojure.string :as string]))

(defn- close? [a b]
  (and (Double/isFinite (double a))
       (Double/isFinite (double b))
       (<= (abs (- (double a) (double b)))
           (max 1.0e-9 (* 1.0e-6 (max (abs (double a)) (abs (double b))))))))

(defn value=
  "Exact structure, keys and non-floating values; floats allow 1e-6 relative
  or 1e-9 absolute error for differences in Spark's numerical backends."
  [expected actual]
  (cond
    (= expected actual) true
    (and (float? expected) (float? actual)) (close? expected actual)
    (and (map? expected) (map? actual))
    (and (= (set (keys expected)) (set (keys actual)))
         (every? (fn [[k v]] (value= v (get actual k))) expected))
    (and (sequential? expected) (sequential? actual))
    (and (= (count expected) (count actual))
         (every? true? (map value= expected actual)))
    :else false))

(def ^:private decimal-token
  #"(?<![\w.])[-+]?(?:\d+\.\d+(?:[eE][-+]?\d+)?|\d+[eE][-+]?\d+)(?![\w.])")

(defn- line= [expected actual]
  (let [table? (and (string/starts-with? expected "|")
                    (string/starts-with? actual "|"))
        normalize (fn [s]
                    (cond
                      (re-matches #"\+(?:-+\+)+" s) (string/replace s #"-+" "-")
                      table? (string/replace s #" +\|" "|")
                      :else s))
        expected (normalize expected)
        actual (normalize actual)]
    (and (= (string/replace expected decimal-token "<float>")
            (string/replace actual decimal-token "<float>"))
         (value= (mapv parse-double (re-seq decimal-token expected))
                 (mapv parse-double (re-seq decimal-token actual))))))

(defn stdout=
  "Compare printed lines with numerical tolerance and Spark table padding.
  Text, integer IDs, row order, column count and line count remain checked."
  [expected actual]
  (and (= (count expected) (count actual))
       (every? true? (map line= expected actual))))
