(ns zero-one.geni.defaults
  "The SparkSession that Geni functions use when they aren't given one.
  Requiring Geni doesn't start Spark: the session is looked up, or created,
  when a function first needs it."
  (:require
   [zero-one.geni.spark :as spark])
  (:import
   (clojure.lang IDeref)
   (org.apache.spark.sql SparkSession)))

(defonce ^:private chosen (atom nil))

(defn set-default-session!
  "Makes `spark` the SparkSession that Geni functions use when they aren't
  given one, and returns it. Pass nil to go back to Spark's active session.

  ```clojure
  (g/set-default-session! (g/create-spark-session {:app-name \"My App\"}))
  ```"
  [spark]
  (when-not (or (nil? spark) (instance? SparkSession spark))
    (throw (ex-info (str "Expected a SparkSession or nil, got " (type spark))
                    {:spark spark})))
  (reset! chosen spark))

(defn- default-session ^SparkSession []
  (or @chosen
      (spark/active-session)
      (locking chosen
        (or (spark/active-session)
            (spark/create-spark-session {})))))

(def spark
  "The default SparkSession, which Geni functions use when they aren't given
  one. Deref it to get the session: the one passed to `set-default-session!`,
  or else Spark's active session, or else a new local one. Geni configures
  only the sessions it creates, and then only as `create-spark-session`
  describes."
  (reify IDeref
    (deref [_] (default-session))))
