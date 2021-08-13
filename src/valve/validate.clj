(ns valve.validate
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            ;; TODO: pprint is used for debugging during dev. Remove this dependency later.
            [clojure.pprint :refer [pprint]]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [clojure.walk :refer [keywordize-keys]]
            [valve.log :as log]
            ;; We need to load this dependency, even if we never explicitly reference it, in order
            ;; to bring the :valve.spec/... namespaces into scope.
            [valve.spec]))

;; Builtin validate functions
(defn validate-any
  "TODO: Insert docstring here"
  ;; TODO: Implement this function
  [])

(defn validate-concat
  "TODO: Insert docstring here"
  ;; TODO: Implement this function
  [])

(defn validate-distinct
  "TODO: Insert docstring here"
  ;; TODO: Implement this function
  [])

(defn validate-in
  "TODO: Insert docstring here"
  ;; TODO: Implement this function
  [])

(defn validate-list
  "TODO: Insert docstring here"
  ;; TODO: Implement this function
  [])

(defn validate-lookup
  "TODO: Insert docstring here"
  ;; TODO: Implement this function
  [])

(defn validate-sub
  "TODO: Insert docstring here"
  ;; TODO: Implement this function
  [])

(defn validate-under
  "TODO: Insert docstring here"
  ;; TODO: Implement this function
  [])

(defn validate-not
  "TODO: Insert docstring here"
  ;; TODO: Implement this function
  [])

;; Builtin check functions:
(defn check-lookup
  "TODO: Insert docstring here"
  ;; TODO: Implement this function
  [])

(def default-datatypes
  {:blank {:datatype "blank"
           :parent ""
           :match #"^$"
           :level "ERROR"}
   :datatype_label {:datatype "datatype_label"
                    :parent ""
                    :match #"[A-Za-z][A-Za-z0-9_-]+"
                    :level "ERROR"}
   :regex {:datatype "regex"
           :parent ""
           :match #"^/.+/$"
           :level "ERROR"}
   :regex_sub {:datatype "regex_sub"
               :parent ""
               :match #"^s/.+[^\\]|.*(?<!/)/.*[^\\]/.+[^\\]|.*(?<!/)/.*[^\\]/.*$"
               :level "ERROR"}})

(def default-functions {:any {"usage" "any(expression+)"
                              "check" ["expression+"]
                              "validate" validate-any}
                        :concat {"usage" "concat(value+)"
                                 "check" ["(expression or string)+"]
                                 "validate" validate-concat}
                        :distinct {"usage" "distinct(expression)"
                                   "check" ["expression" "field*"]
                                   "validate" validate-distinct}
                        :in {"usage" "in(value+)"
                             "check" ["(string or field)+" "named:match_case?"]
                             "validate" validate-in}
                        :list {"usage" "list(str, expression)"
                               "check" ["string" "expression"]
                               "validate" validate-list}
                        :lookup {"usage" "lookup(table, column, column)"
                                 "check" check-lookup
                                 "validate" validate-lookup}
                        :not {"usage" "not(expression)"
                              "check" ["expression"]
                              "validate" validate-not}
                        :sub {"usage" "sub(regex, expression)"
                              "check" ["regex_sub" "expression"]
                              "validate" validate-sub}
                        :tree {"usage" "tree(column, [treename, named=bool])"
                               "check" ["column" "field?" "named:split?"]
                               "validate" nil}
                        :under {"usage" "under(treename, str, [direct=bool])"
                                "check" ["field" "string" "named:direct?"]
                                "validate" validate-under}})

(def datatype-conditions
  [[:datatype "datatype_label"],
   [:parent "any(blank, in(datatype.datatype))"]
   [:match "any(blank, regex)"]
   [:level "any(blank, in(\"ERROR\", \"error\", \"WARN\", \"warn\", \"INFO\", \"info\"))"]
   [:replace "any(blank, regex_sub)"]])

(defn validate
  "TODO: Insert docstring here"
  ([paths custom-functions custom-namespace distinct-messages row-start]
   (when (and custom-functions
              (not (instance? java.util.Map custom-functions)))
     (throw (Exception. "Value for custom-functions must be a map")))

   (letfn [(check-custom [[func-name details]]
             (when (contains? default-functions func-name)
               (throw (Exception.
                       (str "Custom function: " func-name " duplicates a builtin function name"))))

             (when-not (contains? details "validate")
               (throw (Exception. (str "Details of " func-name " must include a 'validate' key"))))

             (let [qualified-name (->> func-name (name) (str custom-namespace "/validate-"))
                   ;; See https://stackoverflow.com/a/1748508/8599709
                   params (->> qualified-name
                               (symbol)
                               (ns-resolve *ns*)
                               (meta)
                               :arglists
                               (#(if (> (count %) 1)
                                   (throw
                                    (Exception.
                                     (str "Custom function: " qualified-name " has "
                                          "more than one argument list")))
                                   (first %)))
                               (map name))]
               (when-not (= (nth params 0) "config")
                 (throw (Exception. (str "'" qualified-name "' argument 1 must be 'config'"))))
               (when-not (= (nth params 1) "args")
                 (throw (Exception. (str "'" qualified-name "' argument 2 must be 'args'"))))
               (when-not (= (nth params 2) "table")
                 (throw (Exception. (str "'" qualified-name "' argument 3 must be 'table'"))))
               (when-not (= (nth params 3) "column")
                 (throw (Exception. (str "'" qualified-name "' argument 4 must be 'column'"))))
               (when-not (= (nth params 4) "row-idx")
                 (throw (Exception. (str "'" qualified-name "' argument 5 must be 'row-idx'"))))
               (when-not (= (nth params 5) "value")
                 (throw (Exception. (str "'" qualified-name "' argument 6 must be 'value'"))))

               ;; After validating the given custom function's details, return them unchanged:
               [func-name details]))

           (check-for-duplicates
             [fixed-paths]
             (when-not (->> fixed-paths
                            (map #(string/split % #"/"))
                            (map last)
                            (map #(string/replace % #"\..*$" ""))
                            (apply distinct?))
               (throw (Exception. "Input paths contain duplicate table names")))
             fixed-paths)

           (get-table-details [fixed-paths]
             (->> fixed-paths
                  (map (fn [path]
                         (with-open [reader (io/reader path)]
                           (let [table-name (-> path (io/file) (.getName)
                                                (string/replace #"\.(c|t)sv$" ""))
                                 sep (if (string/ends-with? path ".csv")
                                       \,
                                       \tab)
                                 [header & data] (doall (csv/read-csv reader :separator sep))
                                 header (->> header (map keyword))
                                 data (if (some #(= table-name %) ["field" "rule" "datatype"])
                                        data
                                        (-> (- row-start 2) (drop data)))]

                             {table-name
                              {:path path
                               :fields (->> header (map keyword))
                               ;; For error reporting purposes we need the order of the headers to
                               ;; be preserved in the generated rows. Zipmap, the simplest way of
                               ;; associating header fields with each row column, does not preserve
                               ;; order, so we need to use the following more complicated method
                               ;; involving array-map instead:
                               :rows (->> data
                                          (map (fn [row]
                                                 (->> row
                                                      (map-indexed (fn [idx column]
                                                                     (array-map (nth header idx)
                                                                                (nth row idx))))
                                                      (apply merge)))))}}))))
                  (apply merge)
                  (keywordize-keys)))

           (check-rows [config spec table rows]
             (let [errors (->> rows
                               (map-indexed
                                (fn [idx row]
                                  (when-not (s/valid? spec row)
                                    (let [{problems ::s/problems
                                           value ::s/value
                                           :as explanation} (s/explain-data spec row)]
                                      ;;(pprint value)
                                      ;;(pprint explanation)
                                      (->> problems
                                           (map (fn [problem]
                                                  (hash-map
                                                   :row idx
                                                   :column (->> problem
                                                                :path
                                                                (first)
                                                                (.indexOf (keys value)))
                                                   :level (->> value
                                                               :level
                                                               (#(if (s/valid? :valve.spec/level %)
                                                                   %
                                                                   "ERROR")))
                                                   :message (str
                                                             (-> problem :path first)  " has value "
                                                             (:val problem) " that does not "
                                                             "conform to " (:pred problem))))))))))

                               (remove nil?))]
               (pprint errors)

               []))

           (configure-datatypes [config]
             (let [datatype (or (-> config :table-details :datatype)
                                (throw (Exception. "Missing table 'datatype'")))
                   rows (:rows datatype)
                   messages (check-rows config :valve.spec/datatype "datatype" rows)
                   ;; to be continued ...
                   ]

               (doseq [row rows]
                 ;; TODO: implement this for loop:
                 )

               messages))]

     (let [;; Register functions:
           functions (->> custom-functions
                          (seq)
                          (map check-custom)
                          (apply concat)
                          (apply hash-map)
                          (merge default-functions)
                          (keywordize-keys))

           ;; Look in the given paths for .tsv and .csv files and collect them into a list. Note
           ;; that we do not need to verify that non-directory paths end in .csv or .tsv, since
           ;; we assume this has already been checked by the calling function.
           fixed-paths (->> paths
                            (map (fn [path]
                                   (if-not (-> path (io/file) (.isDirectory))
                                     (list path)
                                     (->> path (io/file) (.list)
                                          (filter #(or (string/ends-with? % ".csv")
                                                       (string/ends-with? % ".tsv")))
                                          (map #(str path "/" %))))))
                            (apply concat))

           ;; Load all tables, error on duplicates
           table-details (-> fixed-paths (check-for-duplicates) (get-table-details))
           config {:functions functions :table-details table-details :row-start row-start
                   :datatypes default-datatypes}

           ;; TODO: Load datatype, field, and rule - stop process on any problems
           setup-messages (configure-datatypes config)

           kill-messages (do)

           ;; TODO: Run validation
           messages (do)]

       ;;(pprint functions)
       ;;(pprint fixed-paths)
       ;;(pprint table-details)
       ;;(pprint setup-messages)

       ;; TODO: Return messages, logging an error if the list is not empty.
       (or messages []))))

  ([paths distinct-messages row-start]
   (validate paths {} nil distinct-messages row-start)))
