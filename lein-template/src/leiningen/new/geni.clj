(ns leiningen.new.geni
  (:require
    [leiningen.new.templates :refer [renderer year date project-name
                                     ->files sanitize-ns name-to-path]]
    [leiningen.core.main :as main]))

(def render (renderer "geni"))

(defn option? [option-name options]
  (boolean (some #{option-name} options)))

(defn geni
  "A geni project template."
  [name & options]
  (let [main-ns (sanitize-ns name)
        data    {:raw-name    name
                 :name        (project-name name)
                 :namespace   main-ns
                 :nested-dirs (name-to-path main-ns)
                 :year        (year)
                 :date        (date)
                 ;; Options
                 :dataproc?   (option? "+dataproc" options)
                 :gsheets?    (option? "+gsheets" options)
                 :xgboost?    (option? "+xgboost" options)}]
    (main/info "Generating a new geni project.")
    (->files data
             [".gitignore" (render "gitignore" data)]
             ["README.md" (render "README.md" data)]
             ["project.clj" (render "project.clj" data)]
             ["src/{{nested-dirs}}/core.clj" (render "core.clj" data)])))
