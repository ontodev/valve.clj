(ns valve.core-test
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.test :refer :all]
            [valve.cli-handler :refer [handle-cli-opts]]
            [valve.validate :refer [validate]]))

(defn- validate-custom-one
  [config args table column row-idx value]
  [])

(defn- validate-custom-two
  [config args table column row-idx value]
  [])

(defn- validate-custom-three
  [config args table row-idx value]
  [])

(defn- validate-custom-four
  [config args table column value]
  [])

(def ^:private valid-custom-functions
  {:custom-one {"usage" "custom-one(expression+)"
                "check" ["expression+"]
                "validate" validate-custom-one}
   :custom-two {"usage" "custom-two(expression+)"
                "check" ["expression+"]
                "validate" validate-custom-two}})

(def ^:private invalid-custom-functions
  {:custom-three {"usage" "custom-three(expression+)"
                  "check" ["expression+"]
                  "validate" validate-custom-three}
   :custom-four {"usage" "custom-four(expression+)"
                 "check" ["expression+"]
                 "validate" validate-custom-four}})

(def ^:private
  paths ["../valve/tests/inputs"])

(deftest test-validate-valid-custom-funcs
  (testing "Validate test"
    ;; There is no assertion test because this test is considered to pass as long as no
    ;; exception is thrown.
    (validate {:paths paths
               :custom-functions valid-custom-functions
               :custom-namespace "valve.core-test"
               :distinct-messages "test/output/distinct"
               :row-start 2})))

(deftest test-validate-invalid-custom-funcs
  (testing "Validate test"
    (is (thrown? Exception
                 (validate {:paths paths
                            :custom-functions invalid-custom-functions
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
