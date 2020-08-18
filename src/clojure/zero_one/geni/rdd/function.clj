(ns zero-one.geni.rdd.function)

(defmacro gen-function [cls wrapper-name]
  `(defn ~wrapper-name [f#]
     (new ~(symbol (str "zero_one.geni.rdd.function." cls)) f#)))

(gen-function Function function)
(gen-function Function2 function2)
;(gen-function Function3 function3)
(gen-function VoidFunction void-function)
(gen-function FlatMapFunction flat-map-function)
;(gen-function FlatMapFunction2 flat-map-function2)
(gen-function PairFlatMapFunction pair-flat-map-function)
(gen-function PairFunction pair-function)
