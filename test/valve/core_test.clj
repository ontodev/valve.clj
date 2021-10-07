(ns valve.core-test
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.test :refer :all]
            [valve.cli-handler :refer [handle-cli-opts]]
            [valve.validate :refer [validate]]))

(defn- validate-custom-1
  [config args table column row-idx value]
  {:abba "gabba"})

(defn- validate-custom-2
  [config args table column row-idx value]
  {:abba "gabba"})

(def ^:private custom-functions
  {:custom-1 {"usage" "any(expression+)"
              "check" ["expression+"]
              "validate" validate-custom-1}
   :custom-2 {"usage" "any(expression+)"
              "check" ["expression+"]
              "validate" validate-custom-2}})

(def ^:private
  paths ["../valve/tests/inputs"])

;; TODO: I'm not sure what the expected output of this test case should be:
(deftest test-validate-custom-funcs
  (testing "Validate test"
    (is (= [] (validate {:paths paths
                         :custom-functions custom-functions
                         :custom-namespace "valve.core-test"
                         :distinct-messages "test/output/distinct"
                         :row-start 2})))))

(deftest test-end-to-end
  (testing "End-to-end test"
    (do
      (-> ["-o" "test/output/output.tsv"]
          (concat paths)
          (handle-cli-opts))
      (let [expected (slurp "test/output/expected.tsv")
            output (slurp "test/output/output.tsv")]
        (is (= expected output))))))

(deftest test-end-to-end-distinct
  (testing "End-to-end test (distinct)"
    (do
      (-> ["-o" "test/output/output.tsv" "-d" "test/output/distinct"]
          (concat paths)
          (handle-cli-opts))
      (let [expected-global (slurp "test/output/expected-distinct.tsv")
            output-global (slurp "test/output/output.tsv")
            expected-tables (->> "test/output/distinct" (io/file) (.list)
                                 (filter #(string/ends-with? % "-expected.tsv"))
                                 (map #(str "test/output/distinct/" %)))
            actual-tables (->> expected-tables (map #(string/replace % #"-expected" "")))
            extra-tables (->> "test/output/distinct" (io/file) (.list)
                              (map #(str "test/output/distinct/" %))
                              (remove (fn [table] (some #(= % table) expected-tables)))
                              (remove (fn [table] (some #(= % table) actual-tables))))]
        (is (= expected-global output-global))
        (is (empty? extra-tables))

        (doseq [expected-table expected-tables]
          (let [actual-table (string/replace expected-table #"-expected" "")
                actual (slurp actual-table)
                expected (slurp expected-table)]
            (is (= actual expected))))))))
