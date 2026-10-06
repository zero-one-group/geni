(ns zero-one.geni.core.udf-artifacts
  "What a Spark Connect server needs to run a Clojure UDF, which goes up
  through the client's `addArtifact`, each piece once per session: Clojure's
  and Geni's jars, the jar or the source directory of each namespace that the
  function uses, and the classes that Clojure kept for it, with `g/connect`'s
  `:keep-classes`, when it was compiled at the REPL. The server loads a
  namespace that has a file from that file, as a cluster's executors load it
  from the application's jar. A session's server keeps the first version of
  each class and file that it gets, so code that changed after it went up
  throws an error, rather than running as it was."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as string])
  (:import
   (clojure.lang Compiler DynamicClassLoader Reflector Var)
   (java.io ByteArrayInputStream DataInputStream File PushbackReader)
   (java.lang.reflect Field Modifier)
   (java.net URL)
   (java.nio.file Files)
   (java.nio.file.attribute FileAttribute)
   (java.util Arrays Collections IdentityHashMap LinkedHashSet Map WeakHashMap)
   (java.util.jar JarEntry JarOutputStream)))

;;;; Kept classes

(defonce ^:private kept-classes (atom nil))

(defn- delete-tree! [^File f]
  (when (.isDirectory f)
    (run! delete-tree! (.listFiles f)))
  (.delete f))

(defn- own-binding?
  "Whether this thread can set `v`: it has no binding of it, or one of its
  own, rather than another thread's, as a future has its caller's."
  [^Var v]
  (or (not (thread-bound? v))
      (try
        (var-set v @v)
        true
        (catch IllegalStateException _ false))))

(defn check-keep-classes!
  "Throws when `keep-classes!` couldn't set Clojure's compile vars for this
  thread, since it has another thread's bindings of them, as a future has
  its caller's. `g/connect` checks before it starts a session."
  []
  (when-not (every? own-binding? [#'*compile-files* #'*compile-path*])
    (throw (ex-info (str "g/connect's :keep-classes sets *compile-files* and *compile-path* for the "
                         "thread that calls it, and this thread has another thread's bindings of "
                         "them, as a future has its caller's. Call g/connect on that thread, such "
                         "as the REPL's.")
                    {}))))

(defn- set-var!
  "Sets the dynamic var's root, and this thread's binding of it, when it has
  one, as a REPL has of `*compile-path*`."
  [^Var v value]
  (alter-var-root v (constantly value))
  (when (thread-bound? v)
    (var-set v value)))

(defn keep-classes!
  "Has Clojure write the classes that it compiles from now on into a
  temporary directory, as `compile` does, so that a UDF of a function
  defined at the REPL can go to a Spark Connect server: what `g/connect`'s
  `:keep-classes` does. It sets the roots of `*compile-files*` and
  `*compile-path*`, so Clojure compiles that way in every thread, and this
  thread's bindings of them. Another thread that binds `*compile-path*`, as
  each REPL does, writes into its own, which has to exist. Returns the
  directory, which goes when the JVM exits."
  ^File []
  (check-keep-classes!)
  (let [dir (locking kept-classes
              (or @kept-classes
                  (let [dir (.toFile (Files/createTempDirectory "geni-classes"
                                                                (make-array FileAttribute 0)))]
                    (.addShutdownHook (Runtime/getRuntime) (Thread. #(delete-tree! dir)))
                    (reset! kept-classes dir))))]
    (set-var! #'*compile-path* (str dir))
    (set-var! #'*compile-files* true)
    dir))

(defn- internal-name
  "A class's name as its class file has it, such as user$f$fn__123."
  [^Class c]
  (string/replace (.getName c) "." "/"))

(defn- kept-file
  "The class file that Clojure kept for the class `internal-name`, if any."
  ^File [internal-name]
  (when-let [^File dir @kept-classes]
    (let [f (io/file dir (str internal-name ".class"))]
      (when (.isFile f) f))))

(defn- class-refs
  "The internal names of the classes that a class file's constant pool
  names, such as user$f$fn__123 for a function that makes one, leaving out
  arrays."
  [^bytes bytes]
  (let [in        (DataInputStream. (ByteArrayInputStream. bytes))
        _         (.skipBytes in 8)
        n         (.readUnsignedShort in)
        utf8      (object-array n)
        class-ids (java.util.ArrayList.)]
    ;; The tags and sizes of a class file's constants (JVM spec 4.4).
    (loop [i 1]
      (when (< i n)
        (let [tag (.readUnsignedByte in)]
          (case (int tag)
            1                       (do (aset utf8 i (.readUTF in)) (recur (inc i)))
            7                       (do (.add class-ids (.readUnsignedShort in)) (recur (inc i)))
            (8 16 19 20)            (do (.skipBytes in 2) (recur (inc i)))
            15                      (do (.skipBytes in 3) (recur (inc i)))
            (3 4 9 10 11 12 17 18)  (do (.skipBytes in 4) (recur (inc i)))
            (5 6)                   (do (.skipBytes in 8) (recur (+ i 2)))))))
    (->> class-ids
         (map #(aget utf8 (int %)))
         (remove #(string/starts-with? % "[")))))

(defn- namespace-kept-files
  "The class files that Clojure kept for the namespace, as {internal-name
  file}."
  [ns-sym]
  (when-let [^File dir @kept-classes]
    (let [prefix (-> (str ns-sym) (string/replace "-" "_") (string/replace "." "/"))
          base   (.toPath dir)]
      (into {}
            (for [^File f (file-seq dir)
                  :when (.isFile f)
                  :let [path (string/replace (str (.relativize base (.toPath f))) File/separator "/")]
                  :when (and (string/ends-with? path ".class")
                             (or (string/starts-with? path (str prefix "$"))
                                 (string/starts-with? path (str prefix "__init"))
                                 (string/starts-with? path (str prefix "/"))
                                 (= path (str prefix ".class"))))]
              [(subs path 0 (- (count path) (count ".class"))) f])))))

(defn- kept-closure
  "The kept class files that the classes need: their own, and those of the
  classes that they name, however deeply, as {internal-name bytes}."
  [classes]
  (loop [todo (mapv internal-name classes)
         seen #{}
         out  {}]
    (if-let [n (first todo)]
      (let [todo (subvec todo 1)]
        (if (seen n)
          (recur todo seen out)
          (if-let [f (kept-file n)]
            (let [bytes (Files/readAllBytes (.toPath f))]
              (recur (into todo (class-refs bytes)) (conj seen n) (assoc out n bytes)))
            (recur todo (conj seen n) out))))
      out)))

;;;; Where code comes from

(defn- resource-path [ns-sym suffix]
  (str (-> (str ns-sym) (string/replace "-" "_") (string/replace "." "/")) suffix))

(defn- resource-root
  "The jar or the classpath directory that holds `resource`, or nil."
  ^File [^String resource]
  (when-let [^URL url (io/resource resource)]
    (case (.getProtocol url)
      "jar"  (io/file (.toURI (URL. (first (string/split (.getPath url) #"!/" 2)))))
      "file" (nth (iterate #(.getParentFile ^File %) (io/file (.toURI url)))
                  (count (string/split resource #"/")))
      nil)))

(defn- namespace-root
  "The jar or the classpath directory that holds the namespace's code."
  ^File [ns-sym]
  (some #(resource-root (resource-path ns-sym %)) ["__init.class" ".clj" ".cljc"]))

(defn- source-url
  "Where the namespace's source is on the classpath, if it's there."
  ^URL [ns-sym]
  (some #(io/resource (resource-path ns-sym %)) [".clj" ".cljc"]))

(defn- source-file
  "The namespace's source file, when it's in a classpath directory."
  ^File [ns-sym]
  (when-let [url (source-url ns-sym)]
    (when (= "file" (.getProtocol url))
      (io/file (.toURI url)))))

(defn- libspec?
  "Whether `x` names one lib, as `require` takes it: a symbol, or a vector
  of a symbol and its options, rather than a prefix list."
  [x]
  (or (symbol? x)
      (and (vector? x) (or (nil? (second x)) (keyword? (second x))))))

(defn- loads?
  "Whether a libspec loads its lib, which `:as-alias` alone doesn't."
  [spec]
  (or (symbol? spec)
      (let [opts (set (filter keyword? spec))]
        (or (not (opts :as-alias)) (boolean (some opts [:as :refer]))))))

(defn- required-libs
  "The libs that an `ns` form's `:require` and `:use` clauses load."
  [ns-form]
  (set (for [clause     (drop 2 ns-form)
             :when      (and (seq? clause) (#{:require :use} (first clause)))
             arg        (rest clause)
             :when      (not (keyword? arg))
             [lib spec] (if (libspec? arg)
                          [[(if (vector? arg) (first arg) arg) arg]]
                          (let [[prefix & specs] arg]
                            (for [spec specs
                                  :when (not (keyword? spec))]
                              [(symbol (str prefix "." (if (vector? spec) (first spec) spec)))
                               spec])))
             :when      (loads? spec)]
         lib)))

(defn- file-requires
  "The libs that the namespace's file loads in its `ns` form."
  [ns-sym]
  (when-let [url (source-url ns-sym)]
    (try
      (with-open [r (PushbackReader. (io/reader url))]
        (let [form (binding [*read-eval* false]
                     (read {:eof nil :read-cond :allow :features #{:clj}} r))]
          (when (and (seq? form) (= 'ns (first form)))
            (required-libs form))))
      (catch Exception _ nil))))

(defn- uses
  "The namespaces that a namespace uses: its aliases', those of the vars it
  refers to, and the libs that its file's `ns` form loads, with or without
  an alias."
  [ns-sym]
  (into (set (file-requires ns-sym))
        (when-let [n (find-ns ns-sym)]
          (concat (map ns-name (vals (ns-aliases n)))
                  (keep #(when (var? %) (ns-name (.ns ^Var %))) (vals (ns-refers n)))))))

(def ^:private base-resources
  "Resources from the jars, or directories, that every UDF needs: Clojure's,
  spec's, and Geni's code, Java classes and resources."
  ["clojure/core__init.class"
   "clojure/spec/alpha.clj"
   "clojure/core/specs/alpha.clj"
   "zero_one/geni/core/udf.clj"
   "zero_one/geni/udf/UdfFn.class"
   "spark-docs.edn"])

(defn- base-roots []
  (set (keep resource-root base-resources)))

(defn- own?
  "Whether the namespace comes in the jars that every UDF needs."
  [ns-sym]
  (contains? (base-roots) (namespace-root ns-sym)))

(defn- needed-namespaces
  "The namespaces that `f` uses, and theirs, but not Clojure's or Geni's."
  [f]
  (loop [todo (vec ((requiring-resolve 'zero-one.geni.rdd.function/namespace-references) f))
         seen #{}]
    (if-let [ns-sym (first todo)]
      (if (or (seen ns-sym) (own? ns-sym))
        (recur (subvec todo 1) seen)
        (recur (into (subvec todo 1) (uses ns-sym)) (conj seen ns-sym)))
      seen)))

(defn- class-namespace
  "The namespace of a class that Clojure compiled, such as `user` for
  `user$eval1$fn__2`, or `my.app` for the record `my.app.Point`."
  [^Class c]
  (let [n (string/replace (.getName c) "_" "-")]
    (symbol (if (string/includes? n "$")
              (first (string/split n #"\$"))
              (string/replace n #"\.[^.]+$" "")))))

(defn- named-in-a-file?
  "Whether the class has a name that the server gives it too, as it loads
  its namespace from the namespace's file: a top-level `defn`'s, such as
  `my.app$f`, where an anonymous function's has a number, as
  `my.app$f$fn__123` has, or a record's or a type's, such as `my.app.Point`."
  [^Class c]
  (let [n (.getName c)]
    (and (or (not (string/includes? n "$"))
             (and (re-matches #"[^$]+\$[^$]+" n) (not (re-find #"__\d+$" n))))
         (some? (namespace-root (class-namespace c))))))

(defn- compiled-at-run-time?
  "Whether the class came from Clojure's compiler in this JVM, rather than
  from a jar or a classpath directory."
  [^Class c]
  (instance? DynamicClassLoader (.getClassLoader c)))

(defn- held-classes
  "The classes, compiled at run time, of `f` and of the functions and other
  objects that it holds, however deeply, as the fields of a function or the
  elements of a collection."
  [f]
  (let [found   (LinkedHashSet.)
        visited (Collections/newSetFromMap (IdentityHashMap.))
        access  (requiring-resolve 'zero-one.geni.rdd.function/access-field)]
    (letfn [(walk [obj]
              (when-not (or (nil? obj) (boolean? obj) (string? obj) (number? obj) (keyword? obj)
                            (symbol? obj) (var? obj) (instance? clojure.lang.Ref obj)
                            ;; A lazy seq could be infinite.
                            (and (seq? obj) (not (list? obj)))
                            (.contains visited obj))
                (.add visited obj)
                (when (compiled-at-run-time? (class obj))
                  (.add found (class obj)))
                (cond
                  (fn? obj)
                  (doseq [^Field field (.getDeclaredFields (class obj))
                          :when (not (Modifier/isStatic (.getModifiers field)))]
                    (walk (access field obj)))

                  (or (coll? obj) (instance? java.util.Collection obj) (instance? Map obj))
                  (run! walk obj)

                  (instance? java.util.Map$Entry obj)
                  (do (walk (key obj)) (walk (val obj))))))]
      (walk f))
    (vec found)))

;;;; Jars of classpath directories

(defonce ^:private dir-jars
  ;; Each classpath directory's jar, and what the directory had when it was
  ;; made: its newest file's time, and how many files.
  (atom {}))

(defn- dir-snapshot [^File dir]
  (let [files (filter #(.isFile ^File %) (file-seq dir))]
    {:time  (reduce max 0 (map #(.lastModified ^File %) files))
     :count (count files)}))

(defn- write-jar! [^File jar ^File dir]
  (let [base (.toPath dir)]
    (with-open [out (JarOutputStream. (io/output-stream jar))]
      (doseq [^File f (file-seq dir)
              :when (.isFile f)]
        (.putNextEntry out (doto (JarEntry. (string/replace (str (.relativize base (.toPath f)))
                                                            File/separator "/"))
                             (.setTime (.lastModified f))))
        (io/copy f out)
        (.closeEntry out)))))

(defn- jar-of-dir
  "A temporary jar of the classpath directory's files, made again when they
  have changed since, as {:jar file :time newest-file-time}."
  [^File dir]
  (locking dir-jars
    (let [snapshot (dir-snapshot dir)
          made     (@dir-jars dir)]
      (if (= snapshot (:snapshot made))
        made
        (let [jar (doto (File/createTempFile "geni-classpath-" ".jar") .deleteOnExit)]
          (write-jar! jar dir)
          (get (swap! dir-jars assoc dir {:jar jar :time (:time snapshot) :snapshot snapshot})
               dir))))))

;;;; Sessions

(defonce ^:private sessions
  ;; What went up to each Spark Connect session: the roots, with the time of
  ;; the newest file that each had, the kept classes, with their bytes, and
  ;; the namespaces that each UDF's function uses.
  (Collections/synchronizedMap (WeakHashMap.)))

(defn- state [session]
  (locking sessions
    (or (.get ^Map sessions session)
        (let [s (atom {:roots   {}
                       :classes {}
                       :udfs    (Collections/synchronizedMap (WeakHashMap.))})]
          (.put ^Map sessions session s)
          s))))

(defn register-session!
  "Notes a session that `g/connect` made, so that UDFs go to it, as well as
  to the default session."
  [session]
  (state session)
  nil)

(defn- call [target method]
  (Reflector/invokeInstanceMethod target method (object-array 0)))

(defn- usable?
  "Whether the session is still open: the server hasn't closed it, and its
  client hasn't, whose closed channel would only retry an upload for minutes."
  [session]
  (try
    (and (boolean (call session "isUsable"))
         (not (call (call (call session "client") "channel") "isShutdown")))
    (catch Exception _ false)))

(defn open-sessions
  "The Spark Connect sessions that `g/connect` made, or that UDFs went to,
  and that are still open."
  []
  (filterv usable? (locking sessions (vec (.keySet ^Map sessions)))))

;;;; Uploads

(defn- changed! [what]
  (throw (ex-info (str what " changed after it went to this Spark Connect session, whose server "
                       "keeps the first version that it gets. Connect again, with g/connect, for "
                       "a session that gets the new one.")
                  {})))

(defn- check-source!
  "Throws when the namespace's file changed after its directory went up."
  [session ns-sym]
  (when-let [^File file (source-file ns-sym)]
    (when-let [time (get-in @(state session) [:roots (namespace-root ns-sym)])]
      (when (> (.lastModified file) time)
        (changed! (str "The file of " ns-sym ", " file ","))))))

(defn- upload-root!
  "Uploads the jar, or the classpath directory as a jar, unless the session
  has it."
  [session ^File root]
  (let [s (state session)]
    (when-not (contains? (:roots @s) root)
      (let [{:keys [^File jar time]} (if (.isDirectory root)
                                       (jar-of-dir root)
                                       {:jar root :time Long/MAX_VALUE})]
        (.addArtifact session (.getAbsolutePath jar))
        (swap! s assoc-in [:roots root] time)))))

(defn- class-changed! [internal-name]
  (let [n (string/replace internal-name "/" ".")]
    (throw (ex-info (str (Compiler/demunge n) ", the class " n ", changed after it went to this "
                         "Spark Connect session, whose server keeps the first version of a class. "
                         "Connect again, with g/connect, for a session that gets the new one, or "
                         "give the function a new name.")
                    {:class n}))))

(defn- upload-class! [session n ^bytes bytes needed?]
  (.addArtifact session bytes (str n ".class"))
  (swap! (state session) assoc-in [:classes n] {:bytes bytes :needed? needed?}))

(defn- upload-classes!
  "Uploads the kept classes that a UDF needs, after checking that none of
  them changed since a UDF that went to the session needed it, and the other
  classes kept for their namespaces that the session doesn't have yet, so
  that the UDFs of their other functions need no more: the server loads its
  classes again after each new one. A class that went only that way, which
  no UDF has run, goes again when it changed."
  [session needed namespaces]
  (let [s (state session)]
    (doseq [[n ^bytes bytes] needed
            :let [{^bytes sent :bytes needed? :needed?} (get-in @s [:classes n])]]
      (when (and needed? (not (Arrays/equals sent bytes)))
        (class-changed! n)))
    (doseq [[n ^bytes bytes] needed
            :let [{^bytes sent :bytes needed? :needed?} (get-in @s [:classes n])]
            :when (not needed?)]
      (if (and sent (Arrays/equals sent bytes))
        (swap! s assoc-in [:classes n :needed?] true)
        (upload-class! session n bytes true)))
    (doseq [ns-sym namespaces
            [n ^File f] (namespace-kept-files ns-sym)
            :when (not (get-in @s [:classes n]))]
      (upload-class! session n (Files/readAllBytes (.toPath f)) false))))

(defn- unkept! [^Class c]
  (let [n (.getName c)]
    (throw (ex-info (str (Compiler/demunge n) ", the class " n ", was compiled at run time, as a "
                         "function at the REPL is, "
                         (if @kept-classes
                           (str "before g/connect's :keep-classes, so its class can't go to the "
                                "Spark Connect server. Define it again, or reload its namespace, ")
                           (str "so its class can't go to the Spark Connect server. Connect with "
                                "{:keep-classes true} and define it again, "))
                         "or pass a var of a namespace in a file, such as #'my.app/my-fn, or a "
                         "function that such a namespace defines with defn.")
                    {:function n}))))

(defn- check-var!
  "Throws when `f` is a var that the server can't load by name, since its
  namespace has no file."
  [f]
  (when (var? f)
    (let [ns-sym (ns-name (.ns ^Var f))]
      (when-not (namespace-root ns-sym)
        (throw (ex-info (str f " goes to the Spark Connect server by name, and its namespace, "
                             ns-sym ", has no file for the server to load. Pass its function, "
                             "which goes as a class, with g/connect's :keep-classes.")
                        {:var (str f)}))))))

(defn- upload-udf-artifacts!
  "Uploads what the server needs to run `f`, and returns the namespaces that
  it uses."
  [session f]
  ;; A var goes to the server by name, and the server loads its namespace.
  ;; A function goes as an object, so the server needs the classes of the
  ;; functions that it holds: ones that Clojure kept, or ones that the
  ;; server makes as it loads their namespaces from their files.
  (check-var! f)
  (let [held       (if (var? f) [] (held-classes f))
        _          (when-let [c (first (remove #(or (kept-file (internal-name %))
                                                    (named-in-a-file? %))
                                               held))]
                     (unkept! c))
        namespaces (->> (into (needed-namespaces f)
                              (if (var? f) [(ns-name (.ns ^Var f))] (map class-namespace held)))
                        (remove own?)
                        (filter namespace-root)
                        vec)]
    (run! #(check-source! session %) namespaces)
    (doseq [root (concat (base-roots) (map namespace-root namespaces))]
      (upload-root! session root))
    (upload-classes! session (kept-closure held) (distinct (map class-namespace held)))
    namespaces))

(defn upload-udf!
  "Uploads to `session` what its Spark Connect server needs to run `f`, a
  function or a var, as a UDF, each artifact once: Clojure's and Geni's jars,
  the jars or source directories of the namespaces that `f` uses, and the
  classes that Clojure kept for the functions that `f` holds. Throws an error
  that says what to do when the server can't get a class that `f` needs,
  and when something that went to the session changed since."
  [session f]
  (let [s (state session)]
    (locking s
      (let [^Map udfs (:udfs @s)]
        (if-let [namespaces (.get udfs f)]
          (run! #(check-source! session %) namespaces)
          (.put udfs f (upload-udf-artifacts! session f)))))))
