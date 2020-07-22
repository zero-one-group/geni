(ns zero-one.geni.repl
  (:require
    [clojure.java.io :as io]
    [nrepl.server]
    [reply.main]))

(def default-opts {:color true :history-file ".nrepl-history"})

(defn client [opts]
  (let [port (or (:port opts) (try
                                (slurp ".nrepl-port")
                                (catch Throwable _)))
        host (or (:host opts) "127.0.0.1")
        opts (assoc (merge default-opts opts) :attach (str host ":" port))]
    (assert (and host port) "host and/or port not specified for REPL client")
    (reply.main/launch-nrepl opts)))

(defn retry
  "Source: https://stackoverflow.com/questions/12068640/retrying-something-3-times-before-throwing-an-exception-in-clojure"
  [retries f & args]
  (let [res (try {:value (apply f args)}
                 (catch Exception e
                   (if (zero? retries)
                     (throw e)
                     {:exception e})))]
    (if (:exception res)
      (recur (dec retries) f args)
      (:value res))))

(defn launch-repl
  ([] (retry 5 #(launch-repl (+ 65001 (rand-int 500)))))
  ([port]
   (let [server (nrepl.server/start-server :port port)]
     (doto (io/file ".nrepl-port") .deleteOnExit (spit port))
     (println (str "nREPL server started on port " port))
     (client {:port port :custom-eval '(ns zero-one.geni.main)})
     (nrepl.server/stop-server server))))
