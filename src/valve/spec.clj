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
