(ns valve.core-test
  (:require [clojure.test :refer :all]
            [valve.cli-handler :refer [handle-cli-opts]]
            [valve.validate :refer [validate]]))

(defn- validate-custom-1
  [config args table column row-idx value]
  {:abba "gabba"})

(defn- validate-custom-2
  [config args table column row-idx value]
  {:abba "gabba"})

(def ^:private custom-functions
  {"custom-1" {"usage" "any(expression+)"
               "check" ["expression+"]
               "validate" validate-custom-1}
   "custom-2" {"usage" "any(expression+)"
               "check" ["expression+"]
               "validate" validate-custom-2}})

(deftest test-validate
  (testing "Validate test"
    (is (= [] (validate ["output.csv"] custom-functions "valve.core-test" "distinct" 2)))))

(deftest test-end-to-end
  (testing "End-to-end test"
    (is (= 0 (handle-cli-opts ["-o" "./output.csv" "-d" "./distinct/" "./input1.csv"])))))
