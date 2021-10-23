(ns valve.core
  (:require [valve.cli-handler :as cli-handler])
  (:gen-class))

(defn -main
  [& args]
  ;; Handle command-line options:
  (let [status (cli-handler/handle-cli-opts args)]
    (System/exit status)))

