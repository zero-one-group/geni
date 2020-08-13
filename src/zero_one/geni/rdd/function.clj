(ns zero-one.geni.rdd.function
  (:require
    [zero-one.geni.rdd.cache :as cache]
    [zero-one.geni.rdd.kryo :as kryo]
    [serializable.fn :as sfn])
  (:import
    [org.apache.spark.api.java.function
     Function
     Function2
     Function3
     VoidFunction
     VoidFunction2
     FlatMapFunction
     FlatMapFunction2
     ForeachPartitionFunction
     PairFlatMapFunction
     PairFunction
     DoubleFunction
     DoubleFlatMapFunction
     MapFunction
     ReduceFunction
     FilterFunction
     MapPartitionsFunction
     MapGroupsFunction
     FlatMapGroupsFunction]))

(def serialize-fn sfn/serialize)
(def deserialize-fn (cache/lru-memoize 5000 sfn/deserialize))

(defn -init [f]
  [[] f])

(defn mk-sym [fmt sym-name]
  (symbol (format fmt sym-name)))

(defmacro gen-function [cls wrapper-name]
  (let [new-class-sym (mk-sym "zero-one.geni.rdd.function.%s" cls)
        prefix-sym    (mk-sym "%s-" cls)]
    `(do
       (gen-class
        :name         ~new-class-sym
        :implements   [~(mk-sym "org.apache.spark.api.java.function.%s" cls)]
        :prefix       ~prefix-sym
        :init         ~'init
        :state        ~'state
        :constructors {[Object] []})
       (def ~(mk-sym "%s-init" cls) -init)
       (defn ~(mk-sym "%s-call" cls) [~'this & ~'args]
         (let [f# (binding [sfn/*deserialize* kryo/deserialize]
                    (deserialize-fn (.state ~'this)))]
           (apply f# ~'args)))
       (defn ~wrapper-name [f#]
         (new ~new-class-sym
              (binding [sfn/*serialize* kryo/serialize]
                (serialize-fn f#)))))))

(gen-function Function function)
(gen-function Function2 function2)
(gen-function Function3 function3)
(gen-function VoidFunction void-function)
(gen-function VoidFunction2 void-function2)
(gen-function FlatMapFunction flat-map-function)
(gen-function FlatMapFunction2 flat-map-function2)
(gen-function PairFlatMapFunction pair-flat-map-function)
(gen-function PairFunction pair-function)
(gen-function DoubleFunction double-function)
(gen-function DoubleFlatMapFunction double-flat-map-function)
(gen-function MapFunction map-function)
(gen-function ReduceFunction reduce-function)
(gen-function FilterFunction filter-function)
(gen-function ForeachPartitionFunction for-each-partition-function)
(gen-function MapPartitionsFunction map-partitions-function)
(gen-function MapGroupsFunction map-groups-function)
(gen-function FlatMapGroupsFunction flat-map-groups-function)
