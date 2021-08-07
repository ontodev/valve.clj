(ns valve.core-test
  (:require [clojure.test :refer :all]
            [valve.cli-handler :refer [handle-cli-opts]]))

(deftest end-to-end
  (testing "Ent-to-ent test"
    (is (= 0 (handle-cli-opts ["-o" "./output.csv" "-d" "./distinct/" "./input1.csv"])))))
