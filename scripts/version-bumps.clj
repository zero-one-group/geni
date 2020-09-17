
(require '[clojure.string :as string])

(def current-version (slurp "resources/GENI_REPL_RELEASED_VERSION"))

(def bumped-version
  (let [[major minor patch] (string/split current-version #".")]
    (string/join "." [major minor (inc patch)])))

(print bumped-version)
