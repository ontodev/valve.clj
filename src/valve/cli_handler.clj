(ns valve.cli-handler
  (:require [clojure.java.io :as io]
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

          (-> (:output options) (io/file) (.isDirectory))
          (binding [*out* *err*]
            (println (:output options) "is a directory. You must specify a CSV or TSV file.")
            (println usage)
            (:error return-status))

          ;; TODO: If the path is not prefixed with './' the following line of code will cause
          ;; a null pointer exception:
          (-> (:output options) (io/file) (.getParent) (io/file) (.canWrite) (not))
          (binding [*out* *err*]
            (println (-> (:output options) (io/file) (.getParent))
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
            (let [messages (validate arguments (:distinct options) (:row-start options))]
              (if-not (empty? messages)
                (do
                  ;; TODO: implement `write-messages`
                  (:error return-status))
                (:success return-status)))
            (catch Exception e
              (binding [*out* *err*]
                (println (.getMessage e))
                (when (= log/config-level log/DEBUG)
                  (.printStackTrace e)))
              (:error return-status))))))
