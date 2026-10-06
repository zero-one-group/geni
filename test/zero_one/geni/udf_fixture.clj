(ns zero-one.geni.udf-fixture
  "A namespace in a file, whose functions udf_test runs as UDFs. Over Spark
  Connect, the server loads it from this file. It requires clojure.set
  without an alias, which the server needs too."
  (:require
   [clojure.set]))

(defn- offset [] 100)

(defn plus-offset [x]
  (+ x (offset)))

(defn new-keys [m seen]
  (clojure.set/difference (set (keys m)) seen))

(defrecord Offset [n])
