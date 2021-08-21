(ns valve.parse
  (:require [clojure.java.io :as io]
            [instaparse.core :as insta]))

(defn parse-condition
  ;; TODO: Implement this function
  [condition]
  (let [condition-parser (-> "valve_grammar.bnf"
                             (io/resource)
                             (insta/parser))]
    (println (str "PARSING CONDITION: '" condition "'"))
    (let [result (-> condition (condition-parser))]
      (if (insta/failure? result)
        (println "FAIL!!!!" (insta/get-failure result))
        (clojure.pprint/pprint result)))
    {:type "string"
     :value "blank"}))
