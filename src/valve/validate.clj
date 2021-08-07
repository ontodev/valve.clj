(ns valve.validate
  (:require [valve.log :as log]))

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


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Custom functions used only for dev.
;; TODO: Remove this section later.
(defn validate-custom-1
  [config args table column row-idx value]
  {:abba "gabba"})

(defn validate-custom-2
  [config args table column row-idx value]
  {:abba "gabba"})

(def custom-functions
  {"custom-1" {"usage" "any(expression+)"
               "check" ["expression+"]
               "validate" validate-custom-1}
   "custom-2" {"usage" "any(expression+)"
               "check" ["expression+"]
               "validate" validate-custom-2}})
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn check-custom-function
  "TODO: Add docstring here"
  [[func-name details]]
  (when (contains? default-functions func-name)
    (throw (Exception. (str "Custom function: " func-name " duplicates a builtin function name"))))

  (when-not (contains? details "validate")
    (throw (Exception. (str "Details of " func-name " must include a 'validate' key"))))

  (let [full-name (str "valve.validate/validate-" func-name)
        ;; See https://stackoverflow.com/a/1748508/8599709
        params (->> full-name
                    (symbol)
                    (ns-resolve *ns*)
                    (meta)
                    :arglists
                    (#(if (> (count %) 1)
                        (throw
                         (Exception.
                          (str "Custom function: " full-name " has "
                               "more than one argument list")))
                        (first %)))
                    (map name))]
    (when-not (= (nth params 0) "config")
      (throw (Exception. (str "'" full-name "' argument 1 must be 'config'"))))
    (when-not (= (nth params 1) "args")
      (throw (Exception. (str "'" full-name "' argument 2 must be 'args'"))))
    (when-not (= (nth params 2) "table")
      (throw (Exception. (str "'" full-name "' argument 3 must be 'table'"))))
    (when-not (= (nth params 3) "column")
      (throw (Exception. (str "'" full-name "' argument 4 must be 'column'"))))
    (when-not (= (nth params 4) "row-idx")
      (throw (Exception. (str "'" full-name "' argument 5 must be 'row-idx'"))))
    (when-not (= (nth params 5) "value")
      (throw (Exception. (str "'" full-name "' argument 6 must be 'value'")))))

  ;; At the end, return the vector back unchanged
  [func-name details])

(defn validate
  "TODO: Insert docstring here"
  ;; TODO: Remove this comment later:
  ;; To test `validate` directly in the repl, send a command like the following:
  ;; (handle-cli-opts ["-o" "./output.csv" "-d" "./distinct/" "./input1.csv"])
  ([paths add-functions distinct-messages row-start]
   (when (and add-functions
              (not (instance? java.util.Map add-functions)))
     (throw (Exception. "Value for add_functions must be a map")))

   (let [;; TODO: remove this override of add-functions later:
         add-functions custom-functions

         functions (->> add-functions
                        (seq)
                        (map check-custom-function)
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

     ;; TODO: Return messages, logging an error if the list is not empty.
     (or messages [])))

  ([paths distinct-messages row-start]
   (validate paths nil distinct-messages row-start)))
