(ns zero-one.geni.repl
  (:require
    [clojure.string]
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

(defn spark-welcome-note [version]
  (clojure.string/join
    "\n"
    ["Spark session available as a Delay object - use `@spark`."
     "Welcome to"
     "      ____              __"
     "     / __/__  ___ _____/ /__"
     "    _\\ \\/ _ \\/ _ `/ __/  '_/"
     (str "   /___/ .__/\\_,_/_/ /_/\\_\\   version " version)
     "      /_/"]))

(defn launch-repl [port custom-eval]
  (let [server (nrepl.server/start-server :port port)]
    (doto (io/file ".nrepl-port") .deleteOnExit (spit port))
    (println (str "nREPL server started on port " port))
    (client {:port port
             :custom-prompt (fn [n] (str "geni-repl (" n ")\nλ "))
             :custom-eval custom-eval})
    (nrepl.server/stop-server server)))
