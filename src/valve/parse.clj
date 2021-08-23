(ns valve.parse
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [instaparse.core :as insta]))

(defn- postprocess
  "TODO: Insert docstring here"
  ;;[[node-type node-value :as result]]
  [result]
  (let [node-type (first result)
        node-values (drop 1 result)
        first-value (first node-values)]
    (binding [*out* *err*]
      ;;(println "Postprocessing" result)
      ;;(println first-value)
      )
    (cond
      (some #(= node-type %) '(:start :expression :label :function-name
                                      :argument :regex :regex-pattern))
      (postprocess first-value)

      (= node-type :string)
      {:type "string"
       :value (postprocess first-value)}

      (= node-type :ALPHANUM)
      (->> result (drop 1) (string/join ""))

      (= node-type :_)
      (when (> (count result) 1)
        {:type "space"
         :value (->> result (drop 1) (string/join ""))})

      (= node-type :dqstring)
      (->> result (drop 1) (string/join ""))

      (= node-type :function)
      {:type "function"
       :name (postprocess first-value)
       :args (postprocess (nth node-values 1))}

      (= node-type :arguments)
      (->> node-values (map postprocess) (vec))

      (= node-type :field)
      {:type "field"
       :table (postprocess first-value)
       :column (postprocess (nth node-values 1))}

      (= node-type :named-arg)
      {:type "named-arg"
       :key (postprocess first-value)
       :value (postprocess (nth node-values 1))}

      (= node-type :regex-sub)
      {:type "regex"
       :pattern (postprocess first-value)
       :replace (-> (nth node-values 1) (postprocess) (string/replace #"\\" ""))
       :flags (postprocess (nth node-values 2))}

      (= node-type :regex-match)
      {:type "regex"
       :pattern (postprocess first-value)
       :flags (postprocess (nth node-values 1))}

      (= node-type :regex-unescaped)
      (->> result (drop 1) (string/join ""))

      :else
      first-value)))

;; TODO: Implement this function
(defn parse-condition
  "TODO: Insert docstring here"
  [condition]
  (let [condition-parser (-> "valve_grammar.bnf"
                             (io/resource)
                             (insta/parser))]
    (println (str "PARSING CONDITION: '" condition "'"))
    (let [result (-> condition (condition-parser))]
      (if (insta/failure? result)
        (println "PARSING FAILED!" (insta/get-failure result))
        ;; Drop the :start keyword and iterate over the remaining nodes in the tree:
        (->> (drop 1 result)
             (map postprocess)
             (remove nil?)
             (vec)
             (clojure.pprint/pprint))))
    {:type "string"
     :value "blank"}))
