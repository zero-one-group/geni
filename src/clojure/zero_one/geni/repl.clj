(ns zero-one.geni.repl
  (:require
    [clojure.string]
    [clojure.java.io :as io]
    [nrepl.server]
    [reply.main]))

(defn- client [opts]
  (let [port (:port opts)
        host (or (:host opts) "127.0.0.1")
        default-opts {:color true :history-file ".nrepl-history"}
        opts (assoc (merge default-opts opts) :attach (str host ":" port))]
    (reply.main/launch-nrepl opts)))

(defn geni-prompt
  "Custom Geni REPL prompt."
  [ns-]
  (str "geni-repl (" ns- ")\nλ "))

(defn launch-repl
  "Starts an nREPL server and steps into a REPL-y."
  [opts]
  (let [port   (:port opts)
        server (nrepl.server/start-server :port port)]
    (doto (io/file ".nrepl-port") .deleteOnExit (spit port))
    (println (str "nREPL server started on port " port))
    (client (merge {:custom-prompt geni-prompt} opts))
    (nrepl.server/stop-server server)))

(defn spark-welcome-note
  "A REPL welcome note, similar to the one in `spark-shell`."
  [version]
  (clojure.string/join
    "\n"
    ["Spark session available as a future object - deref with `@spark`."
     "Welcome to"
     "      ____              __"
     "     / __/__  ___ _____/ /__"
     "    _\\ \\/ _ \\/ _ `/ __/  '_/"
     (str "   /___/ .__/\\_,_/_/ /_/\\_\\   version " version)
     "      /_/"]))
