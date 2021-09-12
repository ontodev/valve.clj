(ns valve.spec
  (:require [clojure.spec.alpha :as s]))

(s/def :valve.spec/level #{"FATAL" "ERROR" "WARN" "INFO" "DEBUG"})

(s/def :valve.spec.datatype/datatype string?)
(s/def :valve.spec.datatype/parent string?)
(s/def :valve.spec.datatype/match string?)
(s/def :valve.spec.datatype/level :valve.spec/level)
(s/def :valve.spec.datatype/message (s/nilable string?))
(s/def :valve.spec.datatype/description (s/nilable string?))
(s/def :valve.spec.datatype/instructions (s/nilable string?))
(s/def :valve.spec.datatype/replace (s/nilable string?))

(s/def :valve.spec/datatype (s/keys :req-un [:valve.spec.datatype/datatype
                                             :valve.spec.datatype/parent
                                             :valve.spec.datatype/match
                                             :valve.spec.datatype/level]))

(s/def :valve.spec.field/table string?)
(s/def :valve.spec.field/column string?)
(s/def :valve.spec.field/condition string?)

(s/def :valve.spec/field (s/keys :req-un [:valve.spec.field/table
                                          :valve.spec.field/column
                                          :valve.spec.field/condition]
                                 :opt-un [:valve.spec/level]))

(s/def :valve.spec.argument.string/type #(= % "string"))
(s/def :valve.spec.argument.string/value string?)

(s/def :valve.spec.argument/string (s/keys :req-un [:valve.spec.argument.string/type
                                                    :valve.spec.argument.string/value]))

(s/def :valve.spec.argument.table-column/type #(= % "field"))
(s/def :valve.spec.argument.table-column/table string?)
(s/def :valve.spec.argument.table-column/column string?)

(s/def :valve.spec.argument/table-column (s/keys :req-un [:valve.spec.argument.table-column/type
                                                          :valve.spec.argument.table-column/table
                                                          :valve.spec.argument.table-column/column]))

(s/def :valve.spec.argument.regex/type #(= % "regex"))
(s/def :valve.spec.argument.regex/pattern string?)
(s/def :valve.spec.argument.regex/replace string?)
(s/def :valve.spec.argument.regex/flags (s/nilable (s/coll-of string?)))

(s/def :valve.spec.argument/regex (s/keys :req-un [:valve.spec.argument.regex/type
                                                   :valve.spec.argument.regex/pattern
                                                   :valve.spec.argument.regex/flags]
                                          :opt-un [:valve.spec.argument.regex/replace]))

(s/def :valve.spec.argument.named-arg/type #(= % "named-arg"))
(s/def :valve.spec.argument.named-arg/key string?)
(s/def :valve.spec.argument.named-arg/value string?)

(s/def :valve.spec.argument/named-arg (s/keys :req-un [:valve.spec.argument.named-arg/type
                                                       :valve.spec.argument.named-arg/key
                                                       :valve.spec.argument.named-arg/value]))

(s/def :valve.spec/argument (s/or :string :valve.spec.argument/string
                                  :table-column :valve.spec.argument/table-column
                                  :regex :valve.spec.argument/regex
                                  :named-arg :valve.spec.argument/named-arg
                                  :function :valve.spec/function))

(s/def :valve.spec.function/type #(= % "function"))
(s/def :valve.spec.function/name string?)
(s/def :valve.spec.function/args (s/coll-of :valve.spec/argument))

(s/def :valve.spec/function (s/keys :req-un [:valve.spec.function/type
                                             :valve.spec.function/name
                                             :valve.spec.function/args]))
