(ns valve.cli-handler
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.tools.cli :refer [parse-opts]]
            [valve.log :as log]
            [valve.validate :refer [validate]]))

(def version
  (str "VALVE Version "
       (->> "project.clj" slurp read-string (drop 2) (cons :version) (apply hash-map) :version)))

(def return-status {:success 0 :error 1})

(def cli-options
  [["-h" "--help"
    "Show detailed command line usage and exit"]
   ["-v" "--version"
    "Print valve version"]
   ["-o" "--output OUTPUT"
    "CSV or TSV to write error messages to (required)"]
   ["-d" "--distinct DISTINCT"
    "Collect each distinct error messages and write to a table in provided directory"]
   ["-r" "--row-start ROW_START"
    "Index of first row in tables to validate"
    :default 2
    :parse-fn #(Integer/parseInt %)
    :validate [#(>= % 0) "Row start parameter must be non-negative."]]])

(defn handle-cli-opts
  "Parses command-line arguments to VALVE and acts accordingly."
  [args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)
        usage (str "Usage: valve -o OUTPUT [OPTIONS] PATH [PATH ...]\n\nOptions:\n" summary)]
    (cond errors
          (binding [*out* *err*]
            (doseq [error errors]
              (println error))
            (println usage)
            (:error return-status))

          (:help options)
          (do (println usage)
              (:success return-status))

          (:version options)
          (do (println version)
              (:success return-status))

          (nil? (:output options))
          (binding [*out* *err*]
            (println "You must specify an output file.")
            (println usage)
            (:error return-status))

          (and (-> (:output options) (string/lower-case) (string/ends-with? ".csv") (not))
               (-> (:output options) (string/lower-case) (string/ends-with? ".tsv") (not)))
          (binding [*out* *err*]
            (println "The output parameter must end with either .csv or .tsv.")
            (println usage)
            (:error return-status))

          ;; This is an edge-case, but it is conceivable that a path ending in .csv or .tsv
          ;; could actually be a directory, so we check for this here:
          (-> (:output options) (io/file) (.isDirectory))
          (binding [*out* *err*]
            (println (:output options) "is a directory. You must specify a CSV or TSV file.")
            (println usage)
            (:error return-status))

          ;; Check to make sure the parent directory ("." if the path includes none) exists and is
          ;; writable:
          (-> (:output options) (io/file) (.getParent) (#(or % ".")) (io/file) (.canWrite) (not))
          (binding [*out* *err*]
            (println (-> (:output options) (io/file) (.getParent) (#(or % ".")))
                     "does not exist or is not writable.")
            (println usage)
            (:error return-status))

          (and (-> (:output options) (io/file) (.exists))
               (-> (:output options) (io/file) (.canWrite) (not)))
          (binding [*out* *err*]
            (println (:output options) "is not writable.")
            (println usage)
            (:error return-status))

          (and (:distinct options)
               (or (-> (:distinct options) (io/file) (.isDirectory) (not))
                   (-> (:distinct options) (io/file) (.canWrite) (not))))
          (binding [*out* *err*]
            (println (:distinct options) "is either not a directory or is not writable.")
            (println usage)
            (:error return-status))

          (empty? arguments)
          (binding [*out* *err*]
            (println "At least one path must be specified.")
            (println usage)
            (:error return-status))

          (->> arguments (some #(-> % (io/file) (.canRead) (not))))
          (binding [*out* *err*]
            (println "Some of the specified paths are not readable.")
            (println usage)
            (:error return-status))

          :else
          (try
            (let [messages (validate {:paths arguments
                                      :distinct-messages (:distinct options)
                                      :row-start 2})]
              (if-not (empty? messages)
                (let [colkeys '(:table :cell :rule-id :rule :level :message :suggestion)
                      get-values-from-rec (fn [db-rec]
                                            (for [colkey colkeys]
                                              (colkey db-rec)))
                      output (:output options)
                      sep (if (string/ends-with? output ".csv")
                            \,
                            \tab)]
                  (with-open [writer (io/writer output)]
                    (csv/write-csv writer [(map name colkeys)] :separator sep)
                    (doseq [rec messages]
                      (csv/write-csv writer [(get-values-from-rec rec)] :separator sep)))
                  ;; Return error status:
                  (:error return-status))
                ;; Return success status
                (:success return-status)))
            (catch Exception e
              (binding [*out* *err*]
                (println (.getMessage e))
                (when (= log/config-level log/DEBUG)
                  (.printStackTrace e)))
              (:error return-status))))))
