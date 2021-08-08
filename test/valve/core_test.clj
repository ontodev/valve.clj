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

(def ^:private
  paths ["test/resources/input1.csv" "test/resources/input2"])

(deftest test-validate-custom-funcs
  (testing "Validate test"
    (is (= [] (validate paths custom-functions "valve.core-test" "test/resources/distinct" 2)))))

(deftest test-end-to-end
  (testing "End-to-end test"
    (-> ["-o" "output.csv" "-d" "test/resources/distinct"]
        (concat paths)
        (handle-cli-opts)
        (#(is (= 0 %))))))
