(ns valve.validate
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            ;; TODO: pprint is used for debugging during dev. Remove this dependency later.
            [clojure.pprint :refer [pprint]]
            [clojure.spec.alpha :as spec]
            [clojure.string :as string]
            [clojure.walk :refer [keywordize-keys]]
            [valve.log :as log]
            [valve.parse :refer [parse-condition]]
            ;; We need to load this dependency, even if we never explicitly reference it, in order
            ;; to bring the :valve.spec/... namespaces into scope.
            [valve.spec]))

;; Builtin validate functions
;; TODO: Implement this function
(defn validate-any
  "TODO: Insert docstring here"
  [])

;; TODO: Implement this function
(defn validate-concat
  "TODO: Insert docstring here"
  [])

;; TODO: Implement this function
(defn validate-distinct
  "TODO: Insert docstring here"
  [])

;; TODO: Implement this function
(defn validate-in
  "TODO: Insert docstring here"
  [])

;; TODO: Implement this function
(defn validate-list
  "TODO: Insert docstring here"
  [])

;; TODO: Implement this function
(defn validate-lookup
  "TODO: Insert docstring here"
  [])

;; TODO: Implement this function
(defn validate-sub
  "TODO: Insert docstring here"
  [])

;; TODO: Implement this function
(defn validate-under
  "TODO: Insert docstring here"
  [])

;; TODO: Implement this function
(defn validate-not
  "TODO: Insert docstring here"
  [])

;; Builtin check functions:
;; TODO: Implement this function
(defn check-lookup
  "TODO: Insert docstring here"
  [config table column args]
  (println "In check-lookup function"))

(def default-datatypes
  {:blank {:datatype "blank"
           :parent ""
           :match #"^$"
           :level "ERROR"}
   :datatype-label {:datatype "datatype-label"
                    :parent ""
                    :match #"[A-Za-z][A-Za-z0-9_-]+"
                    :level "ERROR"}
   :regex {:datatype "regex"
           :parent ""
           :match #"^/.+/$"
           :level "ERROR"}
   :regex-sub {:datatype "regex-sub"
               :parent ""
               :match #"^s/.+[^\\]|.*(?<!/)/.*[^\\]/.+[^\\]|.*(?<!/)/.*[^\\]/.*$"
               :level "ERROR"}})

(def default-functions {:any {"usage" "any(expression+)"

                              ;; TODO: Changed temporarily for dev:
                              ;;"check" ["expression+"]
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
                              "check" ["regex-sub" "expression"]
                              "validate" validate-sub}
                        :tree {"usage" "tree(column, [treename, named=bool])"
                               "check" ["column" "field?" "named:split?"]
                               "validate" nil}
                        :under {"usage" "under(treename, str, [direct=bool])"
                                "check" ["field" "string" "named:direct?"]
                                "validate" validate-under}})

(def datatype-conditions
  [;; Used only for dev:
   ;;[:parent "any(datatype.parent, foo, bar, lookup(blue, grue))"]
   [:parent "any(blank)"]

   ;; Commented out temporarily for dev:
   ;;[:datatype "datatype-label"],
   ;;[:parent "any(blank, in(datatype.datatype))"]
   ;;[:match "any(blank, regex)"]
   ;;[:level "any(blank, in(\"ERROR\", \"error\", \"WARN\", \"warn\", \"INFO\", \"info\"))"]
   ;;[:replace "any(blank, regex-sub)"]
   ])

(defn- check-custom
  "TODO: Insert docstring here"
  [[func-name details custom-namespace]]
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

(defn- check-for-duplicates
  "TODO: Insert docstring here"
  [fixed-paths]
  (when-not (->> fixed-paths
                 (map #(string/split % #"/"))
                 (map last)
                 (map #(string/replace % #"\..*$" ""))
                 (apply distinct?))
    (throw (Exception.
            (str "Input paths: " (vec fixed-paths) " contain duplicate table names"))))
  fixed-paths)

(defn- get-table-details
  "TODO: Insert docstring here"
  [fixed-paths row-start]
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

(defn- idx-to-a1
  "TODO: Insert docstring here"
  [row-num col-num]
  (loop [divisor col-num
         column-label ""]
    (let [[divisor modulus] [(quot divisor 26) (mod divisor 26)]
          [divisor modulus] (if (= 0 modulus)
                              [(- divisor 1) 26]
                              [divisor modulus])
          column-label (-> modulus
                           (+ 64)
                           (char)
                           (str column-label))]
      (if (= 0 divisor)
        (str column-label row-num)
        (recur divisor column-label)))))

(defn- check-rows
  "TODO: Insert docstring here"
  [config spec table-name rows row-start]
  (let [messages
        (->> rows
             (map-indexed
              (fn [idx row]
                (when-not (spec/valid? spec row)
                  (let [{problems ::spec/problems
                         value ::spec/value} (spec/explain-data spec row)]
                    (->> problems
                         (map
                          (fn [problem]
                            (hash-map
                             :table table-name
                             :cell (let [col-num (->> problem
                                                      :path
                                                      (first)
                                                      (.indexOf (keys value))
                                                      (+ 1))
                                         row-num (if (some #(= table-name %)
                                                           ["field" "rule" "datatype"])
                                                   idx
                                                   (+ row-start idx))]
                                     (idx-to-a1 row-num col-num))
                             :level (->> value
                                         :level
                                         (#(if (spec/valid? :valve.spec/level %)
                                             %
                                             "ERROR")))
                             :message (str
                                       (-> problem :path first)  " has value "
                                       (:val problem) " that does not "
                                       "conform to " (:pred problem))
                             :suggestion (:suggestion value)))))))))
             (remove nil?)
             (flatten))]
    ;; Return the list of error messages:
    messages))

(defn- parsed-to-str
  "TODO: Insert docstring here"
  [config condition]
  (log/debug "Unparsing condition:" condition)

  (let [cond-type (:type condition)]
    (cond
      (= cond-type "string")
      (or (->> config :datatypes (keys)
               (filter #(= (name %) (:value condition)))
               (map name)
               (first))
          (str "\"" (:value condition) "\""))

      (= cond-type "field")
      (str (:table condition) "." (:column condition))

      (= cond-type "named-arg")
      (str (:key condition) "=" (:value condition))

      (= cond-type "regex")
      (let [pattern (:pattern condition)
            replace (:replace condition)
            flags (:flags condition)]
        (if replace
          (str "s/" pattern "/" replace "/" flags)
          (str "/" pattern "/" flags)))

      (= cond-type "function")
      (->> (:args condition)
           (map #(parsed-to-str config %))
           (string/join ", ")
           (#(str (:name condition) "(" % ")")))

      :else
      (throw (Exception. (str "Unknown condition type: " cond-type))))))

(defn- check-arg
  "TODO: Add docstring here"
  [config table arg expected]
  (cond
    (.contains expected " or ")
    (let [;; Remove optional parentheses:
          expected (string/replace expected #"^\(|\)$" "")
          ;; Extract the list of disjuncts:
          disjuncts (string/split expected #" or ")]
      (loop [remaining-disjuncts disjuncts
             errors []]
        (if (empty? remaining-disjuncts)
          ;; If we have gone through all the disjuncts, then if there are any errors, construct
          ;; the corresponding error string and return it:
          (when-not (empty? errors)
            (string/join " or " errors))
          ;; Otherwise check the next disjunct:
          (let [err (->> (first remaining-disjuncts)
                         (check-arg config table arg))]
            (if-not (nil? err)
              ;; If we got an error for this disjunct, add it to the list of errors and go on to
              ;; the next disjunct:
              (recur (drop 1 remaining-disjuncts)
                     (if (nil? err)
                       errors
                       (conj errors err)))
              ;; If this disjunct is valid, then the whole expression is. Empty the list of
              ;; remaining disjuncts to check, as well as any previous errors encountered:
              (recur nil nil))))))

    (string/starts-with? expected "named:")
    (let [narg (subs expected 6)]
      (cond (not= (:type arg) "named-arg")
            (str "value must be a named argument '" narg "'")

            (not= (:key arg) narg)
            (str "named argument must be '" narg "'")))

    (= expected "column")
    (cond (not= (:type arg) "string")
          (str "value must be a string representing a column in '" table "'")

          (->> config :table-details (#(get % (keyword table))) :fields
               (not-any? #(= (-> arg :value (keyword))
                             %)))
          (str "'" (:value arg) "' must be a column in '" table "'"))

    (= expected "expression")
    (cond (not-any? #(= (:type arg) %) '("function" "string"))
          (str "value must be a function or datatype")

          (and (= (:type arg) "string")
               (not (contains? (:datatypes config)
                               (-> arg :value (keyword)))))
          (str "'" (:value arg) "' must be a defined datatype"))

    (some #(= expected %) '("field" "string"))
    (when (not= (:type arg) expected)
      (str "value must be a " expected))

    (or (= expected "regex-sub") (= expected "regex-match"))
    (cond (not= (:type arg) "regex")
          (str "value must be a regex pattern")

          (and (= expected "regex-sub")
               (not (contains? arg :replace)))
          (str "regex pattern requires a substitution")

          (and (= expected "regex-match")
               (contains? arg :replace))
          (str "regex pattern should not have a substitution"))

    :else
    (str "Unknown argument type: " expected)))

(defn- check-args
  "TODO: Add docstring here"
  [config function args table-name column condition-name]
  (log/debug "Checking args" args)
  (let [check (:check function)
        error-str (cond
                    (nil? check)
                    ;; Do nothing if the function has no associated check:
                    (do)

                    (fn? check)
                    (check config table-name column args)

                    (not (vector? check))
                    (throw (Exception. (str "'check' value for " condition-name
                                            " must be a list or function")))

                    :else
                    (loop [i 0
                           allowed-args 0
                           errors []
                           e (first check)
                           check (drop 1 check)
                           add-msg ""
                           break? false]
                      (if (or (not e) break?)
                        ;; If there are no more elements to check:
                        (let [error-str (->> errors
                                             (#(if (< (+ i allowed-args) (count args))
                                                 (conj % (str "expects " i " argument(s), "
                                                              "but " (count args) " were given"))
                                                 %))
                                             (string/join "; "))]
                          (when-not (empty? error-str)
                            (str condition-name " " error-str)))

                        ;; Otherwise ... TODO: add comment here.
                        (cond
                          ;; Zero or more:
                          (string/ends-with? e "*")
                          (letfn [(check-zero-or-more [idx e]
                                    (loop [idx idx
                                           errors errors]
                                      (let [args (-> (- (count args) idx) (take-last args))
                                            a (first args)]
                                        (if-not a
                                          [idx errors]
                                          (let [err (check-arg config table-name a e)
                                                errors (if-not err
                                                         errors
                                                         (conj errors (str "optional argument "
                                                                           (+ idx 1) " " err)))]
                                            (recur (+ idx 1)
                                                   errors))))))]
                            (let [e (-> (count e) (- 1) (#(subs e 0 %)))]
                              (let [[i errors] (check-zero-or-more i e)]
                                (recur (+ i 1)
                                       allowed-args
                                       errors
                                       (first check)
                                       (drop 1 check)
                                       add-msg
                                       false))))

                          ;; One or more:
                          (string/ends-with? e "+")
                          (letfn [(check-one-or-more [idx e]
                                    (loop [idx idx
                                           add-msg add-msg
                                           errors errors
                                           break? false]
                                      (let [args (-> (- (count args) idx) (take-last args))
                                            a (first args)]
                                        (if-not a
                                          [idx add-msg errors break?]
                                          (let [err (check-arg config table-name a e)]
                                            (if err
                                              (if (first check)
                                                [idx (str " or " err) errors true]
                                                (recur (+ idx 1)
                                                       add-msg
                                                       (conj errors (str "argument "
                                                                         (+ idx 1) " " err add-msg))
                                                       false))
                                              (recur (+ idx 1)
                                                     add-msg
                                                     errors
                                                     false)))))))]
                            (let [e (-> (count e) (- 1) (#(subs e 0 %)))]
                              (if (<= (count args) i)
                                (recur nil nil (conj errors (str "requires one or more '" e
                                                                 "' at argument " (+ i 1)))
                                       nil nil nil true)
                                (let [[i add-msg errors break?] (check-one-or-more i e)]
                                  (recur (+ i 1)
                                         allowed-args
                                         errors
                                         (first check)
                                         (drop 1 check)
                                         add-msg
                                         break?)))))

                          ;; Zero or one:
                          (string/ends-with? e "?")
                          (letfn [(check-zero-or-one [idx e]
                                    (let [err (check-arg config table-name (first args) e)
                                          allowed-args (+ allowed-args 1)]
                                      (if err
                                        (if (first check)
                                          [allowed-args (str " or " err) errors]
                                          [allowed-args add-msg
                                           (conj errors
                                                 (str "argument " (+ idx 1) " " err add-msg))]))))]
                            (if-not (>= (count args) i)
                              (recur (+ i 1)
                                     allowed-args
                                     errors
                                     (first check)
                                     (drop 1 check)
                                     add-msg
                                     true)
                              (let [e (-> (count e) (- 1) (#(subs e 0 %)))
                                    [allowed-args add-msg errors] (check-zero-or-one i e)]
                                (recur (+ i 1)
                                       allowed-args
                                       errors
                                       (first check)
                                       (drop 1 check)
                                       add-msg
                                       false))))

                          ;; exactly one
                          :else
                          (if (<= (count args) i)
                            (recur nil nil (conj errors (str "requires one '" e
                                                             "' at argument " (+ i 1)))
                                   nil nil nil true)
                            (let [err (check-arg config table-name (first args) e)
                                  errors (if-not err
                                           errors
                                           (conj errors (str "argument " (+ i 1) " " err)))]
                              (recur (+ i 1)
                                     allowed-args
                                     errors
                                     (first check)
                                     (drop 1 check)
                                     add-msg
                                     false)))))))]
    (when error-str
      (throw (Exception. error-str)))))

(defn- check-function
  "TODO: Add docstring here"
  [config table-name column parsed]
  (let [function-name (:name parsed)
        function (-> (:functions config)
                     (get (keyword function-name)))
        args (:args parsed)

        validate-args
        (fn []
          ;; TODO: Add comment here
          (doseq [arg args]
            (cond
              (= (:type arg) "function")
              (let [err (check-function config table-name column arg)]
                (when err (throw (Exception. err))))

              (= (:type arg) "field")
              (let [table (:table arg)
                    column (:column arg)]
                (when-not (-> config :table-details (get (keyword table)))
                  (throw (Exception. (str "unrecognized table '" table "'"))))
                (when (->> config :table-details (#(get % (keyword table)))
                           :fields (not-any? #(= (keyword column) %)))
                  (throw (Exception. (str "unrecognized column '" column
                                          "' in table '" table "'"))))))))]
    (cond
      (nil? function)
      (str "unrecognized function '" function-name "'")

      (spec/valid? :valve.spec.function/args args)
      (try
        (validate-args)
        (check-args config function args table-name column function-name)
        (catch Exception e
          (let [condition (parsed-to-str config parsed)
                except-msg (string/replace e #"java\.lang\.Exception: " "")]
            (str "Arguments: (" (->> args (map #(get % :value)) (string/join ", ")) ") to function "
                 "'" (:usage function) "' are not valid: " except-msg))))

      :else
      (spec/explain-str :valve.spec.function/args args))))

(defn- validate-condition
  "TODO: Add docstring here"
  [config condition table-name column row-idx value]
  ;; TODO: Implement this function
  (cond
    (= (:type condition) "function")
    (do)

    (= (:type condition) "string")
    (do)

    :else
    (throw (Exception. (str "Invalid condition: '" condition "'"))))

  [])

(defn- build-condition
  "TODO: Insert docstring here"
  [config table-name column condition]
  (let [parsed (parse-condition condition)]
    (cond
      (= (:type parsed) "function")
      (let [err (check-function config table-name column parsed)]
        (when err (throw (Exception. err))))

      (= (:type parsed) "string")
      (when-not (-> config :datatypes (contains? (-> parsed :value keyword)))
        (throw (Exception. (str "Unrecognised datatype '" (:value parsed) "'"))))

      :else
      (throw (Exception. (str "Invalid condition '" condition "'"))))
    ;; Return the parsed condition:
    parsed))

(defn- check-config-contents
  "TODO: Add docstring here"
  [config table-name conditions rows]
  (let [parsed-conditions (->> conditions
                               (seq)
                               (map (fn [[column condition]]
                                      [column (build-condition config table-name
                                                               column condition)])))
        row-idx (:row-start config)
        messages (->> (for [row rows]
                        (->> parsed-conditions
                             (seq)
                             (map-indexed
                              (fn [idx [column condition]]
                                (validate-condition config condition table-name column
                                                    (-> config :row-start (+ idx))
                                                    (-> row (get (keyword column))))))
                             (flatten)))
                      (flatten))]
    ;; Return any generated error messages:
    messages))

(defn- configure-datatypes
  "TODO: Add docstring here"
  [config row-start]
  (let [datatype (or (-> config :table-details :datatype)
                     (throw (Exception. "Missing table 'datatype'")))
        rows (:rows datatype)
        config (-> config (assoc :datatypes default-datatypes))
        messages (-> (check-rows config :valve.spec/datatype "datatype" rows row-start)
                     (concat (check-config-contents config "datatype"
                                                    datatype-conditions rows)))]
    ;; Loop through the datatype records and add the corresponding datatype information to our
    ;; configuration:
    (loop [config config
           rows rows]
      (let [dt-rec (first rows)]
        (if-not (nil? dt-rec)
          ;; More records to process:
          (let [match (:match dt-rec)
                dt-rec (if (empty? match)
                         dt-rec
                         (->> match
                              (re-find #"/(.+)/")
                              (second)
                              (re-pattern)
                              (assoc dt-rec :match)))
                dt-key (-> dt-rec :datatype (keyword))]
            (recur (->> (assoc {} dt-key dt-rec)
                        (merge (:datatypes config))
                        (assoc config :datatypes))
                   (drop 1 rows)))
          ;; We are done. Return the new config as well as any error messages:
          [config messages])))))

;; TODO: Implement this function
(defn- configure-fields
  "TODO: Add docstring here"
  [config]
  (let [messages []]
    [config messages]))

;; TODO: Implement this function
(defn- configure-rules
  "TODO: Add docstring here"
  [config]
  (let [messages []]
    [config messages]))

(defn validate
  "TODO: Insert docstring here"
  ([paths custom-functions custom-namespace distinct-messages row-start]
   (when (and custom-functions
              (not (instance? java.util.Map custom-functions)))
     (throw (Exception. "Value for custom-functions must be a map")))

   (let [;; Register functions:
         functions (->> custom-functions
                        (seq)
                        (map #(conj % custom-namespace))
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
         table-details (-> fixed-paths (check-for-duplicates) (get-table-details row-start))

         ;; Set up the initial configuration:
         config {:functions functions :table-details table-details :row-start row-start}

         ;; Load datatype, field, and rule configuration - stop process on any problems
         ;; IN PROGRESS:
         [config setup-messages] (configure-datatypes config row-start)
         ;; TODO NEXT:
         [config setup-messages] (configure-fields config)
         [config setup-messages] (configure-rules config)

         ;; TODO:
         kill-messages (do)

         ;; TODO: Run validation
         messages (do)]

     ;;(pprint functions)
     ;;(pprint fixed-paths)
     ;;(pprint table-details)
     ;;(pprint setup-messages)

     ;; TODO: Return messages, logging an error if the list is not empty.
     (or messages [])))

  ([paths distinct-messages row-start]
   (validate paths {} nil distinct-messages row-start)))
