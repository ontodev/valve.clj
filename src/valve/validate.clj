(ns valve.validate
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            ;; TODO: pprint is used for debugging during dev. Remove this dependency later.
            [clojure.pprint :refer [pprint]]
            [clojure.string :as string]
            [valve.log :as log]))

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

(def default-functions {"any" {"usage" "any(expression+)"
                               "check" ["expression+"]
                               "validate" validate-any}
                        "concat" {"usage" "concat(value+)"
                                  "check" ["(expression or string)+"]
                                  "validate" validate-concat}
                        "distinct" {"usage" "distinct(expression)"
                                    "check" ["expression" "field*"]
                                    "validate" validate-distinct}
                        "in" {"usage" "in(value+)"
                              "check" ["(string or field)+" "named:match_case?"]
                              "validate" validate-in}
                        "list" {"usage" "list(str, expression)"
                                "check" ["string" "expression"]
                                "validate" validate-list}
                        "lookup" {"usage" "lookup(table, column, column)"
                                  "check" check-lookup
                                  "validate" validate-lookup}
                        "not" {"usage" "not(expression)"
                               "check" ["expression"]
                               "validate" validate-not}
                        "sub" {"usage" "sub(regex, expression)"
                               "check" ["regex_sub" "expression"]
                               "validate" validate-sub}
                        "tree" {"usage" "tree(column, [treename, named=bool])"
                                "check" ["column" "field?" "named:split?"]
                                "validate" nil}
                        "under" {"usage" "under(treename, str, [direct=bool])"
                                 "check" ["field" "string" "named:direct?"]
                                 "validate" validate-under}})

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

             (let [qualified-name (str custom-namespace "/validate-" func-name)
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

           (get-table-details [fixed-paths]
             (->> fixed-paths
                  (map (fn [path]
                         (with-open [reader (io/reader path)]
                           (let [table-name (-> path (io/file) (.getName)
                                                (string/replace #"\.(c|t)sv$" ""))
                                 sep (if (string/ends-with? path ".csv")
                                       \,
                                       \tab)
                                 [header & data] (doall (csv/read-csv reader :separator sep))]
                             {table-name
                              {:path path
                               :fields header
                               :rows (if (some #(= table-name %) ["field" "rule" "datatype"])
                                       (->> data (map #(zipmap header %)))
                                       (->> row-start (- 2) (#(drop % data))
                                            (map #(zipmap header %))))}}))))))

           (check-for-duplicates [table-details]
             ;; TODO: Implement this function to verify that no table appears more than once
             ;; in the given list of table details.
             table-details)]

     (let [;; Register functions:
           functions (->> custom-functions
                          (seq)
                          (map check-custom)
                          (apply concat)
                          (apply hash-map)
                          (merge default-functions))

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
           table-details (-> (get-table-details fixed-paths) (check-for-duplicates))
           config {:functions functions :table-details table-details :row-start row-start}

           ;; TODO: Load datatype, field, and rule - stop process on any problems
           setup-messages (do)
           kill-messages (do)

           ;; TODO: Run validation
           messages (do)]

       (pprint config)

       ;; TODO: Return messages, logging an error if the list is not empty.
       (or messages []))))

  ([paths distinct-messages row-start]
   (validate paths {} nil distinct-messages row-start)))
