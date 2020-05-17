# Geni Examples

## Dataframe

## MLlib

For the following examples, we will assume the following required namespaces:

```
(require '[zero-one.geni.core :as g])
(require '[zero-one.geni.ml :as ml])
```

### Correlation

```clojure
(def corr-df
  (g/table->dataset
    spark
    [[[1.0 0.0 -2.0 0.0]]
     [[4.0 5.0 0.0  3.0]]
     [[6.0 7.0 0.0  8.0]]
     [[9.0 0.0 1.0  0.0]]]
    [:features]))

(let [corr-kw (keyword "pearson(features)")]
  (corr-kw (g/first (ml/corr corr-df "features"))))
; => ((1.0                  0.055641488407465814 0.9442673704375603  0.1311482458941057)
;     (0.055641488407465814 1.0                  0.22329687826943603 0.9428090415820635)
;     (0.9442673704375603   0.22329687826943603  1.0                 0.19298245614035084)
;     (0.1311482458941057   0.9428090415820635   0.19298245614035084 1.0))
```

### Hypothesis Testing

```clojure
(def hypothesis-df
  (g/table->dataset
     spark
     [[0.0 [0.5 10.0]]
      [0.0 [1.5 20.0]]
      [1.0 [1.5 30.0]]
      [0.0 [3.5 30.0]]
      [0.0 [3.5 40.0]]
      [1.0 [3.5 40.0]]]
     [:label :features]))

(g/first (ml/chi-square-test hypothesis-df "features" "label"))
; => {:pValues (0.6872892787909721 0.6822703303362126),
;     :degreesOfFreedom (2 3),
;     :statistics (0.75 1.5))
```
