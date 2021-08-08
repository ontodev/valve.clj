(ns valve.validate
  (:require ;; TODO: pprint is used for debugging during dev. Remove this dependency later.
            [clojure.pprint :refer [pprint]]
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
               [func-name details]))]

     (let [;; Register functions:
           functions (->> custom-functions
                          (seq)
                          (map check-custom)
                          (apply concat)
                          (apply hash-map)
                          (merge default-functions))

           ;; TODO: Check for directories and list their entire contents
           fixed-paths (do)
           ;; TODO: Load all tables, error on duplicates
           table-details (do)
           config (do)
           ;; TODO: Load datatype, field, and rule - stop process on any problems
           setup-messages (do)
           kill-messages (do)
           ;; TODO: Run validation
           messages (do)]

       (clojure.pprint/pprint functions)

       ;; TODO: Return messages, logging an error if the list is not empty.
       (or messages []))))

  ([paths distinct-messages row-start]
   (validate paths {} nil distinct-messages row-start)))
