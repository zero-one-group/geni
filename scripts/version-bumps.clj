#!/usr/bin/env bb

(require '[clojure.string :as string])
(require '[clojure.edn :as edn])

(def released-version-path
  "resources/GENI_REPL_RELEASED_VERSION")

(def project-clj-path
  "project.clj")

(def template-project-clj-path
  "lein-template/project.clj")

(def generated-project-clj-path
  "lein-template/resources/leiningen/new/geni/project.clj")

(def docker-deps-edn-path
  "docker/deps.edn")

(def example-clj-app-deps-edn-path
  "examples/geni-clj-app/deps.edn")

;; Bump GENI_RELEASED_VERSION
(def current-released-version
  (string/replace (slurp released-version-path) #"\n" ""))

(def bumped-released-version
  (let [version-numbers (string/split current-released-version #"\.")
        [major minor patch] (map edn/read-string version-numbers)]
    (string/join "." [major minor (inc patch)])))

;; Bump project.clj
(def project-name "zero.one/geni")

(defn dep-str [project-name version]
  (str project-name " \"" version "\""))

(def current-project-clj (slurp project-clj-path))

(def bumped-project-clj
  (string/replace current-project-clj
                  (re-pattern (dep-str project-name current-released-version))
                  (dep-str project-name bumped-released-version)))

;; Bump lein-template/project.clj
(def current-template-project-clj (slurp "lein-template/project.clj"))

(def template-name "geni/lein-template")

(def bumped-template-project-clj
  (string/replace current-template-project-clj
                  (re-pattern (dep-str template-name current-released-version))
                  (dep-str template-name bumped-released-version)))

;; Bump lein-template's generated project.clj
(def current-generated-project-clj
  (slurp generated-project-clj-path))

(def bumped-generated-project-clj
  (string/replace current-generated-project-clj
                  (re-pattern (dep-str project-name current-released-version))
                  (dep-str project-name bumped-released-version)))

;; deps.edn
(defn mvn-str [version]
  (str ":mvn/version \"" version "\""))

(def current-docker-deps-edn (slurp docker-deps-edn-path))

(def bumped-docker-deps-edn
  (string/replace current-docker-deps-edn
                  (re-pattern (mvn-str current-released-version))
                  (mvn-str bumped-released-version)))

(def current-example-clj-app-deps-edn (slurp example-clj-app-deps-edn-path))

(def bumped-example-clj-app-deps-edn
  (string/replace current-example-clj-app-deps-edn
                  (re-pattern (mvn-str current-released-version))
                  (mvn-str bumped-released-version)))

;; Write the changes
(spit project-clj-path bumped-project-clj)
(spit template-project-clj-path bumped-template-project-clj)
(spit generated-project-clj-path bumped-generated-project-clj)
(spit released-version-path bumped-released-version)
(spit docker-deps-edn-path bumped-docker-deps-edn)
(spit example-clj-app-deps-edn-path bumped-example-clj-app-deps-edn)
