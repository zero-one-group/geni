(ns zero-one.geni.rdd.cache
  (:import
    [com.google.common.cache Cache CacheBuilder]))

(defn lru-memoize [size f & _]
  (let [^CacheBuilder builder (doto (CacheBuilder/newBuilder)
                                (.maximumSize (long size)))
        ^Cache c (.build builder)]
    (fn [& args]
      (if-let [v (.getIfPresent c args)]
        v
        (when-let [v (apply f args)]
          (.put c args v)
          v)))))
