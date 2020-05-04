(ns zero-one.geni.utils)

;; Source: https://github.com/techascent/tech.parallel/blob/master/src/tech/parallel/utils.clj
(defmacro export-symbols
  [src-ns & symbol-list]
  `(do
     (require '~src-ns)
     ~@(->> symbol-list
            (mapv
             (fn [sym-name]
               `(let [varval# (requiring-resolve (symbol ~(name src-ns)
                                                         ~(name sym-name)))
                      var-meta# (meta varval#)]
                  (when (:macro var-meta#)
                    (throw
                     (ex-info
                      (format "Cannot export macros as this breaks aot: %s"
                              '~sym-name)
                      {:symbol '~sym-name})))
                  (def ~(symbol (name sym-name)) @varval#)
                  (alter-meta! #'~(symbol (name sym-name))
                               merge
                               (select-keys var-meta#
                                            [:file :line :column
                                             :doc
                                             :column :tag
                                             :arglists]))))))))
