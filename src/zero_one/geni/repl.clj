(ns zero-one.geni.repl
  (:require
    [clojure.string]
    [clojure.java.io :as io]
    [nrepl.server]
    [reply.main]))

(def default-opts {:color true :history-file ".nrepl-history"})

(defn client [opts]
  (let [port (:port opts)
        host (or (:host opts) "127.0.0.1")
        opts (assoc (merge default-opts opts) :attach (str host ":" port))]
    (when-not (:dummy opts)
      (reply.main/launch-nrepl opts))))

(defn geni-prompt [ns-]
  (str "geni-repl (" ns- ")\nλ "))

(defn launch-repl [opts]
  (let [port   (:port opts)
        server (nrepl.server/start-server :port port)]
    (doto (io/file ".nrepl-port") .deleteOnExit (spit port))
    (println (str "nREPL server started on port " port))
    (client (assoc opts :custom-prompt geni-prompt))
    (nrepl.server/stop-server server)))

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

