(ns valve.validate
  (:require [valve.log :as log]))

;; Builtin functions
(defn validate-any
  "TODO: Insert docstring here"
  [])

(defn validate-concat
  "TODO: Insert docstring here"
  [])

(defn validate-distinct
  "TODO: Insert docstring here"
  [])

(defn validate-in
  "TODO: Insert docstring here"
  [])

(defn validate-list
  "TODO: Insert docstring here"
  [])

(defn validate-lookup
  "TODO: Insert docstring here"
  [])

(defn validate-sub
  "TODO: Insert docstring here"
  [])

(defn validate-under
  "TODO: Insert docstring here"
  [])

(defn validate-not
  "TODO: Insert docstring here"
  [])

(defn check-lookup
  "TODO: Insert docstring here"
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


;; TODO: Remove this comment later:
;; To test `validate` directly in the repl, send a command like the following:
;; (handle-cli-opts ["-o" "./output.csv" "-d" "./distinct/" "./input1.csv"])


(defn validate
  "TODO: Insert docstring here"
  ([paths add-functions distinct-messages row-start]
   (log/debug "paths" paths)
   (log/debug "add-functions" add-functions)
   (log/debug "row-start" row-start)
   (log/debug "distinct-messages" distinct-messages)

   (let [;; TODO: Register functions
         functions (do)
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
