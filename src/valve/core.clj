(ns valve.core
  (:require [valve.cli-handler :as cli-handler])
  (:gen-class))

(defn -main
  [& args]
  ;; Handle command-line options:
  (cli-handler/handle-cli-opts args))

