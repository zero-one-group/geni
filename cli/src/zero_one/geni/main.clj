(ns zero-one.geni.main
  (:require
   [clojure.java.io]
   [clojure.pprint]
   ;; Required so that the uberjar compiles it ahead of time, with main.
   [zero-one.geni.core]
   [zero-one.geni.defaults]
   [zero-one.geni.repl :as repl])
  (:gen-class))

;; Removes the pesky ns warning that takes up the first line of the REPL.
(require '[net.cgrand.parsley.fold])

(def spark
  "Geni's default SparkSession. Deref it with `@spark`."
  zero-one.geni.defaults/spark)

(def init-eval
  "The initial form evaluated when the REPL starts up."
  '(do
     (require 'zero-one.geni.main
              '[zero-one.geni.core :as g]
              '[zero-one.geni.ml :as ml]
              '[zero-one.geni.rdd :as rdd])
     (def spark zero-one.geni.main/spark)))

(defn- custom-stream [script-path]
  (try
    (-> (str (slurp script-path) "\nexit\n")
        (.getBytes "UTF-8")
        (java.io.ByteArrayInputStream.))
    (catch Exception _
      (println (str "Cannot find file " script-path "!"))
      (System/exit 1))))

(defn -main
  "The Geni CLI entrypoint.

  It does the following:
  - Prints a welcome note.
  - Launches an nREPL server, which writes to `.nrepl-port` for a
    text editor to connect to.
  - Starts a REPL(-y).
  "
  [& args]
  (println (repl/spark-welcome-note (.version @spark)))
  (let [script-path (first args)]
    (repl/launch-repl (merge {:port (+ 65001 (rand-int 500))
                              :custom-eval init-eval}
                             (when script-path
                               {:input-stream (custom-stream script-path)}))))
  (System/exit 0))
