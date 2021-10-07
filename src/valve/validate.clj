(ns valve.validate
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            ;; TODO: pprint is used for debugging during dev. Remove this dependency later.
            [clojure.pprint :refer [pprint]]
            [clojure.set :refer [rename-keys]]
            [clojure.spec.alpha :as spec]
            [clojure.string :as string]
            [clojure.walk :refer [keywordize-keys]]
            [valve.log :as log]
            [valve.parse :refer [parse]]
            ;; We need to load this dependency, even if we never explicitly reference it, in order
            ;; to bring the :valve.spec/... namespaces into scope.
            [valve.spec]))

(defn- idx-to-a1
  "TODO: Insert docstring here"
  [row col]
  (loop [div col
         column-label ""]
    (let [[div mod] [(quot div 26) (mod div 26)]
          [div mod] (if (= 0 mod)
                      [(- div 1) 26]
                      [div mod])
          column-label (-> mod
                           (+ 64)
                           (char)
                           (str column-label))]
      (if (= 0 div)
        (str column-label row)
        (recur div column-label)))))

(defn- error
  "TODO: Add docstring here"
  [{:keys [config table column row-idx message level suggestion]
    :or {level "ERROR"}}]
  (let [row-start (:row-start config)
        col-idx (-> config
                    :table-details
                    (get (keyword table))
                    :fields
                    (.indexOf (keyword column)))
        row-num (if (some #(= table %) ["datatype" "field" "rule"])
                  row-idx
                  (+ row-idx row-start))
        err {:table table
             :cell (idx-to-a1 row-num (+ 1 col-idx))
             :level level
             :message message}]
    (if suggestion
      (assoc err :suggestion suggestion)
      err)))

(defn- parsed-to-str
  "TODO: Insert docstring here"
  [config condition]
  (let [cond-type (:type condition)]
    (cond
      (= cond-type "string")
      (or (->> config :datatypes (keys)
               (filter #(= (name %) (:value condition)))
               (map name)
               (first))
          (str "\"" (:value condition) "\""))

      (= cond-type "field")
      (let [table (if (-> condition :table (.contains " "))
                    (str "\"" (:table condition) "\"")
                    (:table condition))
            column (if (-> condition :column (.contains " "))
                     (str "\"" (:column condition) "\"")
                     (:column condition))]
        (str table "." column))

      (= cond-type "named-arg")
      (let [val (if (-> condition :value (.contains " "))
                  (str "\"" (:value condition) "\"")
                  (:value condition))]
        (str (:key condition) "=" val))

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

(defn- update-message
  "TODO: Add docstring here"
  [config table column row-idx condition value message]
  (-> message
      (string/replace #"\{table\}" table)
      (string/replace #"\{column\}" (name column))
      (string/replace #"\{row-idx\}" (str row-idx))
      (string/replace #"\{condition\}" (parsed-to-str config condition))
      (string/replace #"\{value\}" value)))

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
             errors []
             valid? false]
        (if (empty? remaining-disjuncts)
          ;; If we have gone through all the disjuncts, then if there are any errors, construct
          ;; the corresponding error string and return it; otherwise nil is returned.
          (when-not valid?
            (string/join " or " errors))
          ;; If we have not gone through all of the disjuncts, check the next one:
          (let [err (->> (first remaining-disjuncts)
                         (check-arg config table arg))]
            (if-not (nil? err)
              ;; If we got an error for this disjunct, add it to the list of errors and go on to
              ;; the next disjunct:
              (recur (drop 1 remaining-disjuncts)
                     (conj errors err)
                     valid?)
              ;; Otherwise, set valid? to true and go on to the next disjunct:
              (recur (drop 1 remaining-disjuncts)
                     errors
                     true))))))

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
  [config function args table column condition-name]
  (let [check (:check function)]
    (cond
      (nil? check)
      ;; Do nothing if the function has no associated check:
      (do)

      (fn? check)
      (check config table column args)

      (not (vector? check))
      (str "'check' value for " condition-name " must be a list or function")

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
                            (let [err (check-arg config table a e)
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

            ;; Zero or one:
            (string/ends-with? e "?")
            (letfn [(check-zero-or-one [idx e]
                      (let [err (check-arg config table (nth args idx) e)
                            allowed-args (+ allowed-args 1)]
                        (if err
                          (if (first check)
                            [(+ i 1) allowed-args errors (first check) (drop 1 check)
                             (str " or " err) false]
                            [i allowed-args (conj errors
                                                  (str "optional argument " (+ idx 1) " "
                                                       err add-msg))
                             e check add-msg true]))))]
              (if (<= (count args) i)
                (recur i
                       allowed-args
                       errors
                       e
                       check
                       add-msg
                       true)
                (let [e (-> (count e) (- 1) (#(subs e 0 %)))
                      [i allowed-args errors e check add-msg break?] (check-zero-or-one i e)]
                  (recur i
                         allowed-args
                         errors
                         e
                         check
                         add-msg
                         break?))))

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
                            [(+ idx 1) allowed-args errors (first check) (drop 1 check) add-msg
                             break?]
                            (let [err (check-arg config table a e)]
                              (if err
                                (if (first check)
                                  [idx allowed-args errors e check (str " or " err) true]
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
                  (recur i
                         allowed-args
                         (conj errors (str "requires one or more '" e
                                           "' at argument " (+ i 1)))
                         e
                         check
                         add-msg
                         true)
                  (let [[i allowed-args errors e check add-msg break?] (check-one-or-more i e)]
                    (recur i
                           allowed-args
                           errors
                           e
                           check
                           add-msg
                           break?)))))

            ;; exactly one
            :else
            (if (<= (count args) i)
              (recur i
                     allowed-args
                     (conj errors (str "requires one '" e
                                       "' at argument " (+ i 1)))
                     e
                     check
                     add-msg
                     true)
              (let [err (check-arg config table (nth args i) e)
                    errors (if-not err
                             errors
                             (conj errors (str "argument " (+ i 1) " " err)))]
                (recur (+ i 1)
                       allowed-args
                       errors
                       (first check)
                       (drop 1 check)
                       add-msg
                       false)))))))))

(defn- check-function
  "TODO: Add docstring here"
  [config table column parsed]
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
              (let [err (check-function config table column arg)]
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
      (do
        (try
          (validate-args)
          (catch Exception e
            (let [condition (parsed-to-str config parsed)
                  except-msg (string/replace e #"java\.lang\.Exception: " "")]
              except-msg)))
        (check-args config function args table column function-name))

      :else
      (spec/explain-str :valve.spec.function/args args))))

(defn- validate-datatype
  "TODO: Add docstring here"
  [config condition table column row-idx value]
  (letfn [(find-ancestors [datatypes datatype]
            (let [parent (-> datatypes (get (keyword datatype)) :parent)]
              (when-not (empty? parent)
                (into [parent] (find-ancestors datatypes parent)))))]
    (let [datatypes (:datatypes config)
          dname (:value condition)
          ancestors (into [dname] (find-ancestors datatypes dname))]
      (loop [ancestors ancestors]
        (let [dname (first ancestors)]
          (if-not dname
            []
            (let [datatype (get datatypes (keyword dname))
                  value (or value "")
                  message (if-not (:message datatype)
                            (str "'" value "' must be of datatype '" dname "'")
                            (update-message config table column row-idx condition value
                                            (:message datatype)))
                  level (get datatype :level "ERROR")
                  match (-> datatype
                            :match
                            (#(when (= (type %) java.util.regex.Pattern)
                                (re-pattern %))))
                  ;; There is nothing in clojure corresponding to Python's re.match(), which matches
                  ;; a pattern against the *start* of a string, so we explicitly make sure to add
                  ;; a caret to force matching from the start. Note that "^" is idempotent so it
                  ;; will do no harm if the string already happens to begin with a caret.
                  match (->> match (str) (str "^") (re-pattern))]
              (when match
                (if (->> value (re-find match) (not))
                  (let [replace (:replace datatype)
                        suggestion (when-not (empty? replace)
                                     (-> replace
                                         ;; TODO: Using string/split in this naive way is not good
                                         ;; because we migth have a string like, e.g.,
                                         ;; s/akjhakjds\\/asdasd/ffasd/g
                                         ;; i.e., note the escaped forward slash.
                                         ;; But it is ok for now.
                                         (string/split #"/")
                                         ((fn [[_ pattern replacement]]
                                            (string/replace value pattern replacement)))))]
                    (-> (error {:config config :table table :column column :row-idx row-idx
                                :message message :level level :suggestion suggestion})
                        (vector)))
                  (recur (drop 1 ancestors)))))))))))

(defn- validate-condition
  "TODO: Add docstring here"
  ([config condition table column row-idx value message]
   (cond
     (= (:type condition) "function")
     (let [args (:args condition)
           func-key (-> condition :name (keyword))
           func (-> config :functions (get func-key) :validate)]
       (func config args table column row-idx value message))

     (= (:type condition) "string")
     (validate-datatype config condition table column row-idx value)

     :else
     (throw (Exception. (str "Invalid condition: '" condition "'")))))

  ([config condition table column row-idx value]
   (validate-condition config condition table column row-idx value nil)))

(defn- parse-condition
  "TODO: Insert docstring here"
  [config table column condition]
  (let [parsed (parse condition)]
    (cond
      (= (:type parsed) "function")
      (let [err (check-function config table column parsed)]
        (if err
          [nil, err]
          [parsed, nil]))

      (= (:type parsed) "string")
      (if-not (-> config :datatypes (contains? (-> parsed :value keyword)))
        [nil, (str "Unrecognised datatype '" (:value parsed) "'")]
        [parsed, nil])

      :else
      [nil, (str "Invalid condition '" condition "'")])))

(defn- build-condition
  "TODO: Insert docstring here"
  [config table column condition]
  (let [[parsed err] (parse-condition config table column condition)]
    (when err (throw (Exception. err)))
    parsed))

(defn- check-config-contents
  "TODO: Add docstring here"
  [config table conditions rows]
  (let [parsed-conditions (->> conditions
                               (seq)
                               (map (fn [[column condition]]
                                      [column (build-condition config table
                                                               column condition)])))
        row-idx (:row-start config)
        messages (->> (for [row rows]
                        (->> parsed-conditions
                             (seq)
                             (map-indexed
                              (fn [idx [column condition]]
                                (validate-condition config condition table column
                                                    (-> config :row-start (+ idx))
                                                    (-> row (get (keyword column))))))
                             (flatten)))
                      (flatten))]

    ;; Return any generated error messages:
    messages))

(defn- get-tree-options
  "TODO: Add docstring here"
  [tree-function]
  (let [args (:args tree-function)]
    (cond
      (-> args (count) (#(or (> % 3) (< % 1))))
      [nil "the `tree` function must have between one and three arguments"]

      ;; First arg is column
      (->> args (first) (#(or (not (instance? java.util.Map %))
                              (-> % :type (not= "string")))))
      [nil "the first argument of the `tree` function must be a column name"]

      :else
      (if (empty? args)
        [{:child-column (-> args (first) :value)
          :add-tree-name nil
          :split-char nil}
         nil]
        (loop [add-tree-name nil
               split-char nil
               x 1]
          (if-not (< x (count args))
            [{:child-column (-> args (first) :value)
              :add-tree-name add-tree-name
              :split-char split-char}
             nil]
            (let [arg (nth args x)]
              (cond
                (and (contains? arg :key) (-> arg :key (= "split")))
                (recur add-tree-name (:value arg) (+ x 1))

                (contains? arg :table)
                (recur (str (:table arg) "." (:column arg)) split-char (+ x 1))

                :else
                [nil (str "`tree` argument " (+ x 1)
                          " must be a table.column pair or split=CHAR")]))))))))

(defn- build-tree
  "TODO: Add docstring here"
  ([config fn-row-idx table-name parent-column child-column add-tree-name split-char]
   (let [table-details (:table-details config)
         row-start (:row-start config)
         rows (-> table-details (get (keyword table-name)) :rows)
         col-idx (-> table-details (get (keyword table-name)) :fields
                     (.indexOf (keyword parent-column)))
         trees (get config :trees {})
         ;; tree is a hash-map of sets:
         tree (get trees (keyword add-tree-name))
         allowed-values (->> rows
                             (map #(get % (keyword child-column)))
                             (concat (keys tree)))
         split-raw (fn [token split-char]
                     ;; Clojure doesn't provide a function for splitting a string other than with
                     ;; a regular expression, so we implement it ourselves:
                     ;; TODO: allow for `split-char` to have length > 1
                     (if-not split-char
                       [token]
                       (loop [token token
                              results []]
                         (let [end-index (.indexOf token split-char)
                               result (subs token 0 (if (= end-index -1)
                                                      (count token)
                                                      end-index))]
                           (if (not= end-index -1)
                             ;; CHANGE (+ 1 to (+ (count split-char) (but don't re-evaluate every time)
                             (recur (subs token (+ 1 end-index) (count token))
                                    (conj results result))
                             (conj results result))))))]

     (if (and add-tree-name (not (contains? trees (keyword add-tree-name))))
       [nil [{:message (str add-tree-name "must be defined before using it in a function")}]]
       (loop [row-idx row-start
              remaining-rows rows
              errors []
              tree tree]
         (if-not (empty? remaining-rows)
           (let [row (first remaining-rows)
                 parent (get row (keyword parent-column))
                 child (get row (keyword child-column))]
             (cond
               (or (nil? parent) (-> parent (string/trim) (empty?)))
               (->> (if (contains? tree (keyword child))
                      tree
                      (assoc tree (keyword child) #{}))
                    (recur (+ row-idx 1) (drop 1 remaining-rows) errors))

               :else
               ;; `parents` and `ancestors` are reserved words in clojure, so we use `folks`
               (let [folks (-> parent (split-raw split-char))
                     errors (->> folks
                                 (map
                                  (fn [parent]
                                    (when (not-any? #(= parent %) allowed-values)
                                      {:table table-name
                                       :cell (idx-to-a1 row-idx (+ col-idx 1))
                                       :rule-id (str "field:" fn-row-idx)
                                       :level "ERROR"
                                       :message (str "'" parent "' from "
                                                     table-name "." parent-column
                                                     " must exist in "
                                                     table-name "." child-column
                                                     (when add-tree-name
                                                       (str " or " add-tree-name " tree")))})))
                                 (concat errors)
                                 (remove nil?))
                     tree (let [curr-parents (get tree (keyword child))]
                            (assoc tree
                                   (keyword child)
                                   (-> tree
                                       (get (keyword child))
                                       (concat folks)
                                       (set))))]
                 (recur (+ row-idx 1) (drop 1 remaining-rows) errors tree))))

           ;; End of loop
           [tree errors])))))

  ([config fn-row-idx table-name parent-column child-column]
   (build-tree config fn-row-idx table-name parent-column child-column nil "|")))

(defn- get-table-details
  "TODO: Insert docstring here"
  ([paths row-start]
   (->> paths
        (map (fn [path]
               (with-open [reader (io/reader path)]
                 (let [table (-> path (io/file) (.getName)
                                 (string/replace #"\.(c|t)sv$" ""))
                       sep (if (string/ends-with? path ".csv")
                             \,
                             \tab)
                       [header & data] (doall (csv/read-csv reader :separator sep))
                       header (->> header (map keyword))
                       data (if (some #(= table %) ["field" "rule" "datatype"])
                              data
                              (-> (- row-start 2) (drop data)))]
                   {table
                    {:path path
                     :fields (->> header (map keyword))
                     ;; For error reporting purposes we need the order of the headers to
                     ;; be preserved in the generated rows. Zipmap, the simplest way of
                     ;; associating header fields with each row column, does not preserve
                     ;; order, so we need to use the following more complicated method
                     ;; involving array-map instead:
                     :rows (->> data
                                (map (fn [row]
                                       (->> header
                                            (map-indexed (fn [idx column]
                                                           (array-map (nth header idx)
                                                                      (if (> idx (- (count row) 1))
                                                                        nil
                                                                        (nth row idx)))))
                                            (apply merge)))))}}))))
        (apply merge)
        (keywordize-keys)))

  ([paths]
   (get-table-details paths 2)))

(defn- validate-table
  "TODO: Insert docstring here"
  ;; Note: `table` is a keyword.
  [config table]
  (let [table-name (name table)
        table-details (:table-details config)
        fields (merge (-> config :table-fields (get table {}))
                      (-> config :table-fields (get :* {})))
        rules (merge (-> config :table-rules (get table {}))
                     (-> config :*))
        rows (-> table-details table :rows)

        check-for-field-type
        (fn [field value row-idx]
          (let [value (or value "")]
            (when (contains? fields field)
              (let [parsed-type (-> fields field :parsed)
                    error-message (-> fields field :message)
                    messages (validate-condition config parsed-type table-name field
                                                 row-idx value error-message)]
                (->> messages
                     (map #(assoc %
                                  :rule-id (->> fields field :field-id
                                                (str "field:"))
                                  :level "ERROR")))))))

        check-for-rules
        (fn [field value row-idx row]
          (when (and (not-empty rules) (contains? rules field))
            (->> rules
                 field
                 (map (fn [rule]
                        (let [whencond (:when-condition rule)
                              messages (validate-condition config whencond table-name field
                                                           row-idx value)]
                          (when (empty? messages)
                            (let [thencol (:column rule)
                                  thenval (or (get row (keyword thencol))
                                              "")
                                  messages (validate-condition config
                                                               (:then-condition rule)
                                                               table-name
                                                               thencol
                                                               row-idx
                                                               thenval
                                                               (:message rule))]
                              (when-not (empty? messages)
                                (->> messages
                                     (map #(let [msg (if (:message rule)
                                                       (:message %)
                                                       (str "because '" value "' is '"
                                                            (parsed-to-str config whencond) "', "
                                                            (:message %)))]
                                             (assoc %
                                                    :rule-id (str "rule:" (:rule-id rule))
                                                    :level (:level rule)
                                                    :message msg)))))))))))))]
    (->> rows
         (map-indexed
          (fn [row-idx row]
            (->> row
                 (map
                  (fn [[field value]]
                    (into (check-for-field-type field value row-idx)
                          (check-for-rules field value row-idx row))))
                 (flatten))))
         (flatten)
         (remove nil?)
         (#(or % '())))))

(defn- collect-distinct-messages
  "TODO: Insert docstring here"
  [table-details output-dir table messages]
  (let [distinct-messages (->> messages
                               (map #(assoc {} (-> % :message (keyword))
                                            %))
                               (apply merge))
        message-rows (->> distinct-messages
                          (seq)
                          (map second)
                          ((fn [messages]
                             (loop [messages messages
                                    message-rows {}]
                               (let [msg (first messages)]
                                 (if msg
                                   (let [row-num (-> msg
                                                     :cell
                                                     (string/replace #"[^\d]+(\d+)$" "$1")
                                                     (Integer/parseInt))
                                         row-messages (get message-rows row-num)]
                                     (recur (drop 1 messages)
                                            (->> msg
                                                 (conj row-messages)
                                                 (assoc message-rows row-num))))
                                   ;; If there are no more messages left to process, return the
                                   ;; generated rows:
                                   message-rows))))))
        basename (-> table (io/file) (.getName))
        [table-name table-ext] (-> basename (string/split #"\." 2))
        sep (if (= "csv" table-ext) \, \tab)
        output (str output-dir "/" table-name "_distinct." table-ext)
        fields (-> table-details (get (keyword table-name)) :fields)
        rows (-> table-details (get (keyword table-name)) :rows)
        get-values-from-row (fn [row]
                              (for [header-key fields]
                                (header-key row)))]

    (log/info (count distinct-messages) "distinct error(s) found in" table)
    (log/info "writing rows with errors to" output)
    (with-open [writer (io/writer output)]
      (csv/write-csv writer (->> fields (map name) (vector))
                     :separator sep)
      (loop [rows rows
             messages []
             row-idx 2
             new-idx 2]
        (let [row (first rows)]
          (if row
            (if (contains? message-rows row-idx)
              (let [messages (->> (get message-rows row-idx)
                                  (map (fn [msg]
                                         (assoc msg
                                                :table (str table-name "_distinct")
                                                :cell (-> msg
                                                          :cell
                                                          (string/replace #"^([^\d]+)\d+" "$1")
                                                          (str new-idx)))))
                                  (concat messages))]
                (csv/write-csv writer [(get-values-from-row row)] :separator sep)
                (recur (drop 1 rows) messages (+ 1 row-idx) (+ 1 new-idx)))
              (recur (drop 1 rows) messages (+ 1 row-idx) new-idx))
            ;; If there are no more rows to process, return the generated messages:
            messages))))))

;; Builtin validate functions
(defn- validate-any
  "TODO: Insert docstring here"
  ([config args table column row-idx value message]
   (let [conditions
         (loop [remaining-args args
                conditions []]
           (let [arg (first remaining-args)]
             (if arg
               (if (empty? (validate-condition config arg table column row-idx value))
                 ;; If any one of the args are valid, then the list as a whole is valid, so send
                 ;; back an empty list of error messages:
                 []
                 ;; Otherwise add the arg to the list of failures:
                 (recur (drop 1 remaining-args)
                        (conj conditions
                              (parsed-to-str config arg))))
               ;; If there are no more args, exit the loop with the list of problematic conditions:
               conditions)))]

     (if (empty? conditions)
       ;; If there were no problematic conditions, then send back an empty list:
       []
       ;; Otherwise ...
       (let [message (if message
                       (update-message config table column row-idx {:type "function"
                                                                    :name "any"
                                                                    :args args}
                                       value message)
                       (str "'" value "' must meet one of: " (string/join ", " conditions)))]
         (vector (error {:config config :table table :column column :row-idx row-idx
                         :message message}))))))

  ([config args table column row-idx value]
   (validate-any config args table column row-idx value nil)))

(defn- validate-concat
  "TODO: Insert docstring here"
  ([config args table column row-idx value message]
   (let [datatypes (:datatypes config)
         [validate-conditions
          validate-values
          messages
          rem] (loop [remaining-args args
                      validate-conditions []
                      validate-values []
                      messages []
                      rem value
                      break? false]
                 (let [arg (first remaining-args)]
                   (if (or break? (not arg))
                     ;; If there are either no more args, or a break has been requested, exit the
                     ;; loop with the results:
                     [validate-conditions validate-values messages rem]
                     ;; Otherwise continue:
                     (if (-> arg :type (= "string"))
                       ;; If the arg type is string:
                       (let [arg-val (:value arg)]
                         (if (contains? datatypes (keyword arg-val))
                           (recur (drop 1 remaining-args)
                                  (conj validate-conditions arg)
                                  validate-values
                                  messages
                                  rem
                                  false)
                           (let [arg-val (-> arg-val (string/replace #"^\"|\"$" ""))
                                 pre (-> (or rem "")
                                         (string/split (re-pattern arg-val) 2)
                                         (first))
                                 rem (-> (or rem "")
                                         (string/split (re-pattern arg-val) 2)
                                         (second))]
                             (cond
                               (empty? rem)
                               (let [message (if message
                                               (update-message config table column row-idx
                                                               {:type "function"
                                                                :name "concat"
                                                                :args args}
                                                               value message)
                                               (str "'" value "' must contain substring '"
                                                    arg-val "'"))]
                                 (recur (drop 1 remaining-args)
                                        validate-conditions
                                        validate-values
                                        (conj messages
                                              (error {:config config :table table :column column
                                                      :row-idx row-idx :message message}))
                                        rem
                                        ;; This error causes us to break out of the loop:
                                        true))

                               (not-empty pre)
                               (recur (drop 1 remaining-args)
                                      validate-conditions
                                      (conj validate-values pre)
                                      messages
                                      rem
                                      false)))))
                       (recur (drop 1 remaining-args)
                              (conj validate-conditions arg)
                              validate-values
                              messages
                              rem
                              false)))))]

     (if-not (empty? messages)
       messages
       ;; Otherwise generate error messages based on the contents of validate-values and
       ;; validate-conditions and return them:
       (let [validate-values (if rem (conj validate-values rem) validate-values)]
         (->> validate-values
              (map-indexed (fn [idx val]
                             (validate-condition config
                                                 (nth validate-conditions idx)
                                                 table
                                                 column
                                                 row-idx
                                                 val
                                                 message)))
              (flatten))))))

  ([config args table column row-idx value]
   (validate-concat config args table column row-idx value nil)))

(defn- validate-distinct
  "TODO: Insert docstring here"
  ([config args table column row-idx value message]
   (let [get-indexes (fn [seekwence item]
                       ;; 'sequence' is a reserved word in clojure.
                       (->> seekwence
                            (map-indexed (fn [idx value]
                                           (when (= value item)
                                             idx)))
                            (remove nil?)))

         table-details (:table-details config)
         row-start (:row-start config)
         base-rows (-> table-details (get (keyword table)) :rows)
         base-headers (-> table-details (get (keyword table)) :fields)
         base-values (->> base-rows (map #(get % (keyword column))))
         value-indexes (get-indexes base-values value)
         duplicate-locs (-> (if (-> value-indexes (count) (> 1))
                              (let [col-idx (-> (.indexOf base-headers (keyword column)) (+ 1))]
                                (->> value-indexes
                                     (filter #(not= % row-idx))
                                     (map #(-> % (+ row-start) (idx-to-a1 col-idx)))
                                     (map #(str table ":" %))
                                     (set)))
                              #{})
                            (into
                             (when (> (count args) 1)
                               (->> args
                                    (drop 1)
                                    (map (fn [item]
                                           (let [t (:table item)
                                                 c (:column item)
                                                 trows (-> table-details (get (keyword t)) :rows)
                                                 theaders (-> table-details (get (keyword t))
                                                              :fields)
                                                 tvalues (->> trows (map #(get % (keyword c))))]
                                             (when (some #(= value %) tvalues)
                                               (let [value-indexes (get-indexes tvalues value)
                                                     col-idx (-> (.indexOf theaders (keyword c))
                                                                 (+ 1))]
                                                 (->> value-indexes
                                                      (map #(-> % (+ row-start) (idx-to-a1
                                                                                 col-idx)))
                                                      (map #(str t ":" %))))))))
                                    (flatten)
                                    (remove nil?)))))]
     (if (empty? duplicate-locs)
       []
       (let [message (if-not (empty? message)
                       (update-message config table column row-idx {:type "function"
                                                                    :name "distinct"
                                                                    :args args}
                                       value message)
                       (str "'" value "' must be distinct with value(s) at: "
                            (string/join ", " duplicate-locs)))]
         (-> (error {:config config :table table :column column :row-idx row-idx :message message})
             (vector))))))

  ([config args table column row-idx value]
   (validate-distinct config args table column row-idx value nil)))

(defn- validate-in
  "TODO: Insert docstring here"
  ([config args table column row-idx value message]
   (let [table-details (:table-details config)
         last-arg (last args)
         match-case (or (-> last-arg :type (not= "named-arg"))
                        (-> last-arg :value (string/lower-case) (not= "false")))
         args (if (-> last-arg :type (not= "named-arg"))
                args
                (take (- (count args) 1) args))
         allowed (loop [remaining-args args
                        allowed []]
                   (let [arg (first remaining-args)]
                     (if-not arg
                       allowed
                       (if (= (:type arg) "string")
                         (if (or (-> arg :value (= value))
                                 (-> arg :value (= (str "\"" value "\""))))
                           ;; Return an empty list:
                           []
                           (recur (drop 1 remaining-args)
                                  (conj allowed
                                        (str "\"" (:value arg) "\""))))
                         (let [table-name (:table arg)
                               column-name (:column arg)
                               source-rows (-> table-details (get (keyword table-name))
                                               :rows)
                               allowed-values (if match-case
                                                (->> source-rows
                                                     (map #(get % (keyword column-name)))
                                                     (remove nil?))
                                                (->> source-rows
                                                     (map #(get % (keyword column-name)))
                                                     (remove nil?)
                                                     (map string/lower-case)))]
                           (if match-case
                             (if (some #(= % value) allowed-values)
                               []
                               (recur (drop 1 remaining-args)
                                      (conj allowed
                                            (str table-name "." column-name))))
                             (if (some #(-> value (string/lower-case) (= %)) allowed-values)
                               []
                               (recur (drop 1 remaining-args)
                                      (conj allowed
                                            (str table-name "." column-name))))))))))]

     (if (empty? allowed)
       []
       (let [message (if-not (empty? message)
                       (update-message config table column row-idx {:type "function"
                                                                    :name "in"
                                                                    :args args}
                                       value message)
                       (str "'" value "' must be in: " (string/join ", " allowed)))]
         (-> (error {:config config :table table :column column :row-idx row-idx :message message})
             (vector))))))

  ([config args table column row-idx value]
   (validate-in config args table column row-idx value nil)))

(defn- validate-list
  "TODO: Insert docstring here"
  ([config args table column row-idx value message]
   (let [split-char-pre (-> args (first) :value)
         ;; This is a bit hacky, perhaps (i.e., taking the enclosing quotes away like this):
         split-char (-> split-char-pre (string/replace #"^\"|\"$" ""))
         expr (second args)
         splits (->> split-char (re-pattern) (string/split value))
         errs (->> splits
                   (map #(validate-condition config expr table column row-idx % message))
                   (flatten))]
     (if-not (empty? errs)
       (->> errs
            (map #(:message %))
            (string/join "; ")
            (error {:config config :table table :column column :row-idx row-idx})
            (vector))
       [])))
  ([config args table column row-idx value]
   (validate-list config args table column row-idx value nil)))

(defn- validate-lookup
  "TODO: Insert docstring here"
  ([config args table column row-idx value message]
   (let [table-details (:table-details config)
         table-rules (-> config :table-rules (get (keyword table)))
         lookup-value (->> table-rules
                           (seq)
                           (map (fn [[whencol rules]]
                                  (loop [remaining-rules rules
                                         lookup-value nil]
                                    (let [rule (first remaining-rules)]
                                      (if-not rule
                                        lookup-value
                                        (if (or (->> rule :column (not= column))
                                                (->> rule :then-condition :name (not= "lookup")))
                                          (recur (drop 1 remaining-rules) lookup-value)
                                          (-> table-details (get (keyword table)) :rows
                                              (nth row-idx) (get (keyword whencol)))))))))
                           (remove nil?)
                           (last))]
     (when (empty? lookup-value)
       (throw
        (Exception.
         (str "Unable to find lookup function for" table "." column "in rule table"))))

     (let [search-table (-> args (first) :value)
           search-column (-> args (second) :value)
           return-column (-> args (nth 2) :value)
           row-errors (loop [remaining-rows (-> table-details (get (keyword search-table)) :rows)]
                        (let [row (first remaining-rows)]
                          (when row
                            (let [maybe-value (get row (keyword search-column))]
                              (if (= maybe-value lookup-value)
                                (let [expected (get row (keyword return-column))]
                                  (if (not= value expected)
                                    (-> (error {:config config
                                                :table table
                                                :column column
                                                :row-idx row-idx
                                                :message (str "'" value "' must be '" expected "'")
                                                :level "ERROR"
                                                :suggestion expected})
                                        (vector))
                                    []))
                                (recur (drop 1 remaining-rows)))))))]
       (if row-errors
         row-errors
         (let [message (if-not (empty? message)
                         (update-message config table column row-idx {:type "function"
                                                                      :name "lookup"
                                                                      :args args}
                                         value message)
                         (str "'" value "' must be present in: " search-table "." return-column))]
           (-> (error {:config config :table table :column column :row-idx row-idx
                       :message message})
               (vector)))))))

  ([config args table column row-idx value]
   (validate-lookup config args table column row-idx value nil)))

(defn- validate-sub
  "TODO: Insert docstring here"
  ([config args table column row-idx value message]
   ;; Note that we assume, as input, perl-style syntax.
   (let [regex (first args)
         subfunc (second args)
         flags (-> regex :flags (string/join))
         [flags global?] (if (and (not-empty flags)
                                  (-> flags (.indexOf "g") (not= -1)))
                           [(string/replace flags #"g" "") true]
                           [flags false])
         pattern (-> (if-not (empty? flags)
                       (->> regex :pattern (str "(?" flags ")"))
                       (:pattern regex))
                     (re-pattern))
         replace (:replace regex)
         value (if global?
                 (string/replace value pattern replace)
                 (string/replace-first value pattern replace))]
     (validate-condition config subfunc table column row-idx value message)))

  ([config args table column row-idx value]
   (validate-sub config args table column row-idx value nil)))

(defn- validate-not
  "TODO: Insert docstring here"
  ([config args table column row-idx value message]
   (loop [remaining-args args]
     (let [arg (first remaining-args)]
       (if-not arg
         []
         (let [messages (validate-condition config arg table column row-idx value)]
           (if-not (empty? messages)
             (recur (drop 1 remaining-args))
             (let [message (if-not (empty? message)
                             (update-message config table column row-idx {:type "function"
                                                                          :name "not"
                                                                          :args args}
                                             value message)
                             (-> (parsed-to-str config arg)
                                 (#(if (= % "blank")
                                     "value must not be blank"
                                     (str "'" value "' must not be '" % "'")))))]
               (vector (error {:config config :table table :column column :row-idx row-idx
                               :message message})))))))))

  ([config args table column row-idx value]
   (validate-not config args table column row-idx value nil)))

(defn- has-ancestor
  "TODO: Insert docstring here"
  ([tree ancestor node direct?]
   (let [folks (get tree (keyword node))
         ;; One could (perhaps) argue that replacing enclosing quotes in this way is hacky ...
         ancestor (string/replace ancestor #"^\"|\"$" "")]
     (cond
       (and (= node ancestor)
            (not direct?))
       true

       (not (-> tree (contains? (keyword node))))
       false

       (contains? folks ancestor)
       true

       direct?
       false

       :else
       (loop [remaining-folks folks]
         (let [folk (first remaining-folks)]
           (if-not folk
             false
             (if (has-ancestor tree ancestor folk)
               true
               (recur (drop 1 remaining-folks)))))))))

  ([tree ancestor node]
   (has-ancestor tree ancestor node false)))

(defn- validate-under
  "TODO: Insert docstring here"
  ([config args table column row-idx value message]
   (let [trees (:trees config)
         table-name (-> args (first) :table)
         column-name (-> args (first) :column)
         tree-name (str table-name "." column-name)]

     (when-not (-> trees (contains? (keyword tree-name)))
       ;; This has already been validated for CLI users
       (throw (Exception. (str "A tree for " tree-name " is not defined"))))

     (let [tree (get trees (keyword tree-name))
           ancestor (-> args (second) :value)
           direct? (boolean
                    (when (= (count args) 3)
                      (-> args (nth 2) :value (string/lower-case) (= "true"))))]
       (if (has-ancestor tree ancestor value direct?)
         []
         (let [message (if-not (empty? message)
                         (update-message config table column row-idx {:type "function"
                                                                      :name "under"
                                                                      :args args}
                                         value message)
                         (str "'" value "' must be "
                              (if-not direct?
                                "equal to or under"
                                "a direct subclass of")
                              " '" ancestor "' from " tree-name))]
           (vector (error {:config config :table table :column column :row-idx row-idx
                           :message message})))))))

  ([config args table column row-idx value]
   (validate-under config args table column row-idx value nil)))

;; Builtin check functions:
(defn- check-lookup
  "TODO: Insert docstring here"
  [config table column args]
  (loop [break? false
         errors []
         i 0
         table nil]
    (if (and (not break?)
             (< i 3)
             (< i (count args)))
      ;; Main loop processing:
      (let [a (nth args i)
            i (+ i 1)]
        (cond
          (-> a :type (not= "string"))
          (recur false
                 (conj errors
                       (str "argument " i " must be of type 'string'"))
                 i
                 table)

          (and (= i 1)
               (-> config :table-details (contains?
                                          (-> a :value (keyword)))
                   (not)))
          (recur true
                 (conj errors "argument 1 must be a table in inputs")
                 i
                 table)

          (= i 1)
          (let [table (:value a)]
            (recur false
                   (if (and table
                            (> i 1)
                            (->> config :table-details (#(get % (keyword table))) :fields
                                 (some #(= % (keyword table)))
                                 (not)))
                     (conj errors
                           (str "argument " i " must be a column in '" table "'"))
                     errors)
                   i
                   table))))
      (if break?
        ;; Return early:
        (->> errors (string/join "; ") (str "lookup "))
        ;; We have now completed the loop:
        (do
          (when-not (= (count args) 3)
            (conj errors
                  (str "expects 3 arguments, but " (count args) " were passed")))
          (when-not (empty? errors)
            (->> errors (string/join "; ") (str "lookup "))))))))

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

(def default-functions
  {:any {"usage" "any(expression+)"
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
  [[:datatype "datatype-label"],
   [:parent "any(blank, in(datatype.datatype))"]
   [:match "any(blank, regex)"]
   [:level "any(blank, in(\"ERROR\", \"error\", \"WARN\", \"warn\", \"INFO\", \"info\"))"]
   [:replace "any(blank, regex-sub)"]])

(def field-conditions
  [[:table "not(blank)"]
   [:column "not(blank)"]
   [:condition "not(blank)"]])

(def rule-conditions
  [[:table "not(blank)"]
   [:when-column "not(blank)"]
   [:when-condition "not(blank)"]
   [:then-column "not(blank)"]
   [:then-condition "not(blank)"]
   [:level "any(blank, in(\"ERROR\", \"error\", \"WARN\", \"warn\", \"INFO\", \"info\"))"]])

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

(defn- check-rows
  "TODO: Insert docstring here"
  [config spec table rows row-start]
  ;; Returns a list of error messages
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
                       :table table
                       :cell (let [col-num (->> problem
                                                :path
                                                (first)
                                                (.indexOf (keys value))
                                                (+ 1))
                                   row-num (if (some #(= table %)
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
                                 "conform to " (:via problem))
                       :suggestion (:suggestion value)))))))))
       (remove nil?)
       (flatten)))

(defn- configure-datatypes
  "TODO: Add docstring here"
  [[config messages] row-start]
  (let [datatype (or (-> config :table-details :datatype)
                     (throw (Exception. "Missing table 'datatype'")))
        rows (:rows datatype)
        config (-> config (assoc :datatypes default-datatypes))
        messages (-> messages
                     (concat (check-rows config :valve.spec/datatype "datatype" rows row-start))
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

(defn- configure-rules
  "TODO: Add docstring here"
  [[config messages] row-start]
  (if-not (-> config :table-details (contains? :rule))
    ;; Rule table is optional. If it is not present just return the config and error messages
    ;; back unchanged:
    [config messages]
    ;; Otherwise configure the rules:
    (let [rows (->> config :table-details :rule :rows
                    (map #(rename-keys % {(keyword "when column") :when-column
                                          (keyword "when condition") :when-condition
                                          (keyword "then column") :then-column
                                          (keyword "then condition") :then-condition})))
          messages (-> messages
                       (concat (check-rows config :valve.spec/rule "rule" rows row-start))
                       (concat (check-config-contents config "rule" rule-conditions rows)))]
      (let [table-rules {}
            row-idx (-> (:row-start config) (- 1))]
        (loop [remaining-rows rows
               row-idx row-idx
               column-rules {}
               table-rules {}
               rules []
               messages messages]
          (if (empty? remaining-rows)
            ;; Once we are done, return the possibly modified config as well as any error messages:
            [(assoc config :table-rules table-rules) messages]
            ;; Otherwise continue the loop:
            (let [row-idx (+ row-idx 1)
                  row (first remaining-rows)
                  table (:table row)
                  column-rules (get table-rules (keyword table) {})
                  whencol (:when-column row)
                  thencol (:then-column row)
                  whencond (:when-condition row)
                  thencond (:then-condition row)
                  rules (get column-rules (keyword whencol) [])]
              (cond
                (-> config :table-details (contains? (keyword table)) (not))
                (->> (conj messages
                           (error {:config config
                                   :table "rule"
                                   :column "table"
                                   :row-idx row-idx
                                   :message (str "unrecognized table '" table "'")}))
                     (recur (drop 1 remaining-rows) row-idx column-rules table-rules rules))

                (->> config :table-details (#(get % (keyword table))) :fields
                     (some #(= (keyword whencol) %)) (not))
                (->> (conj messages
                           (error {:config config
                                   :table "rule"
                                   :column "when column"
                                   :row-idx row-idx
                                   :message (str "unrecognized column '" whencol
                                                 "' for table '" table "'")}))
                     (recur (drop 1 remaining-rows) row-idx column-rules table-rules rules))

                (->> config :table-details (#(get % (keyword table))) :fields
                     (some #(= (keyword thencol) %)) (not))
                (->> (conj messages
                           (error {:config config
                                   :table "rule"
                                   :column "then column"
                                   :row-idx row-idx
                                   :message (str "unrecognized column '" thencol
                                                 "' for table '" table "'")}))
                     (recur (drop 1 remaining-rows) row-idx column-rules table-rules rules))

                :else
                (let [[parsed-when err-when] (parse-condition config table whencol whencond)
                      [parsed-then err-then] (parse-condition config table thencol thencond)]
                  (cond
                    err-when
                    (->> (conj messages (error {:config config
                                                :table "rule"
                                                :column "then condition"
                                                :row-idx row-idx
                                                :message err-when}))
                         (recur (drop 1 remaining-rows) row-idx column-rules table-rules rules))

                    err-then
                    (->> (conj messages (error {:config config
                                                :table "rule"
                                                :column "then condition"
                                                :row-idx row-idx
                                                :message err-then}))
                         (recur (drop 1 remaining-rows) row-idx column-rules table-rules rules))

                    :else
                    (let [rules (conj rules
                                      {:when-condition parsed-when
                                       :column thencol
                                       :then-condition parsed-then
                                       :level (get row :level "ERROR")
                                       :message (get row :message)
                                       :rule-id row-idx})
                          column-rules (assoc column-rules (keyword whencol) rules)
                          table-rules (assoc table-rules (keyword table) column-rules)]
                      (recur (drop 1 remaining-rows) row-idx column-rules table-rules rules
                             messages))))))))))))

(defn- configure-fields
  "TODO: Add docstring here"
  [[config messages] row-start]
  (when-not (-> config :table-details (contains? :field))
    (throw (Exception. "missing table 'field'")))

  (let [rows (-> config :table-details :field :rows)
        fields (-> config :table-details :field :fields)
        messages (-> messages
                     (concat (check-rows config :valve.spec/field "field" rows row-start))
                     (concat (check-config-contents config "field" field-conditions rows)))
        row-idx (-> config :row-start (- 1))]
    (loop [remaining-rows rows
           row-idx row-idx
           trees {}
           table-fields {}
           config config
           messages messages]
      (if (empty? remaining-rows)
        ;; Once we are done, return the possibly modified config as well as any error messages:
        [config messages]
        ;; Otherwise continue to loop:
        (let [row-idx (+ row-idx 1)
              row (first remaining-rows)
              table (:table row)
              column (:column row)
              field-types (-> table-fields (get (keyword table)
                                                {}))]
          (cond
            (and (not= table "*")
                 (-> config :table-details (contains? (keyword table)) (not)))
            (->> (conj messages
                       (error {:config config
                               :table "field"
                               :column "table"
                               :row-idx row-idx
                               :message (str "unrecognized table '" table "'")}))
                 (recur (drop 1 remaining-rows) row-idx trees table-fields config))

            (and (not= table "*")
                 (->> config :table-details (#(get % (keyword table)))
                      :fields (map name) (some #(= column %)) (not)))
            (->> (conj messages
                       (error {:config config
                               :table "field"
                               :column "column"
                               :row-idx row-idx
                               :message (str "unrecognized column '" column "' for table '"
                                             table "'")}))
                 (recur (drop 1 remaining-rows) row-idx trees table-fields config))

            ;; Check that this table.column pair has not already been defined:
            (contains? field-types (keyword column))
            (->> (conj messages
                       (error {:config config
                               :table "field"
                               :column "column"
                               :row-idx row-idx
                               :message (str "Multiple conditions defined for '" table "."
                                             column "'")}))
                 (recur (drop 1 remaining-rows) row-idx trees table-fields config))

            :else
            (let [[parsed-condition err] (->> (:condition row)
                                              (parse-condition config table column))]
              (cond
                err
                (->> (conj messages
                           (error {:config config
                                   :table "field"
                                   :column "condition"
                                   :row-idx row-idx
                                   :message err}))
                     (recur (drop 1 remaining-rows) row-idx trees table-fields config))

                (and (-> parsed-condition :type (= "function"))
                     (-> parsed-condition :name (= "tree")))
                (let [[tree-opts err] (get-tree-options parsed-condition)]
                  (if err
                    (->> (conj messages
                               (error {:config config
                                       :table "field"
                                       :column "condition"
                                       :row-idx row-idx
                                       :message err}))
                         (recur (drop 1 remaining-rows) row-idx trees table-fields config))
                    (let [[tree errs] (build-tree config row-idx table column
                                                  (:child-column tree-opts)
                                                  (:add-tree-name tree-opts)
                                                  (:split-char tree-opts))
                          config (if tree
                                   (->> (str table "." column)
                                        (keyword)
                                        (#(hash-map % tree))
                                        (assoc config :trees))
                                   config)
                          messages (if-not (empty? errs)
                                     (->> errs
                                          (map #(if (contains? % :table)
                                                  %
                                                  (assoc %
                                                         :table "field"
                                                         :cell (idx-to-a1 row-idx
                                                                          (.indexOf
                                                                           fields "condition"))
                                                         :rule "tree function error"
                                                         :level "ERROR")))
                                          (concat messages))
                                     messages)]
                      (recur (drop 1 remaining-rows) row-idx trees table-fields config messages))))

                :else
                (let [field-types (assoc field-types
                                         (keyword column) {:parsed parsed-condition
                                                           :field-id row-idx
                                                           :message (:message row)})
                      table-fields (assoc table-fields
                                          (keyword table) field-types)
                      config (assoc config :table-fields table-fields)]
                  (recur (drop 1 remaining-rows)
                         row-idx trees table-fields config messages))))))))))

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

(defn validate
  "TODO: Insert docstring here"
  [{:keys [paths custom-functions custom-namespace distinct-messages row-start]
    :or {row-start 2}}]
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
        [config setup-messages] (-> [config []]
                                    (configure-datatypes row-start)
                                    (configure-fields row-start)
                                    (configure-rules row-start))

        kill-messages (->> setup-messages
                           (filter (fn [msg]
                                     (some #(= (:table msg) %) '("datatype" "field" "rule")))))]

    (if-not (empty? kill-messages)
      (do
        (log/error "VALVE configuration completed with" (count kill-messages) "errors")
        kill-messages)
      (let [messages
            (->> table-details
                 (keys)
                 (filter (fn [table] (not-any? #(= table %) '(:datatype :field :rule))))
                 (map (fn [table]
                        (log/info "Validating" table)
                        (let [add-messages (->> setup-messages
                                                (filter #(= (name table) (:table %)))
                                                (concat (validate-table config table)))]
                          (log/info (count add-messages) "problems found in table" table)
                          (if (and (not-empty add-messages) (not-empty distinct-messages))
                            (let [table-path (-> table-details (get table) :path)
                                  update-errors (collect-distinct-messages table-details
                                                                           distinct-messages
                                                                           table-path
                                                                           add-messages)]
                              update-errors)
                            add-messages))))
                 (apply concat))]

        (when-not (empty? messages)
          (log/error "VALVE completed with" (count messages) "problems found!"))

        messages))))
