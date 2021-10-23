(ns valve.log)

(def DEBUG 0)
(def INFO 1)
(def WARN 2)
(def ERROR 3)
(def CRITICAL 4)

(def log-levels {:debug DEBUG :info INFO :warn WARN :warning WARN :error ERROR :critical CRITICAL})

;; TODO: Make this configurable
(def config-level (:debug log-levels))

(defn- screened-out?
  "Given a keword representing the log-level, check to see whether the application configuration
  requires it to be screened out."
  [log-level]
  (try
    (let [given-level (log-level log-levels)]
      (< given-level config-level))
    (catch Exception e
      (binding [*out* *err*]
        (println "FATAL cannot determine logging level")
        (System/exit 1)))))

(defn- log
  "Log the message represented by the given words preceeded by the date and time to stderr."
  [first-word & other-words]
  (let [now (-> "yyyy-MM-dd HH:mm:ss.SSSZ"
                (java.text.SimpleDateFormat.)
                (.format (new java.util.Date)))
        message (->> other-words
                     (clojure.string/join " ")
                     (str first-word " "))]
    (binding [*out* *err*]
      (println now "-" message))))

(defn debug
  [first-word & other-words]
  (when (not (screened-out? :debug))
    (apply log "DEBUG" first-word other-words)))

(defn info
  [first-word & other-words]
  (when (not (screened-out? :info))
    (apply log "INFO" first-word other-words)))

(defn warn
  [first-word & other-words]
  (when (not (screened-out? :warn))
    (apply log "WARN" first-word other-words)))

(defn warning
  [first-word & other-words]
  (when (not (screened-out? :warning))
    (apply log "WARN" first-word other-words)))

(defn error
  [first-word & other-words]
  (when (not (screened-out? :error))
    (apply log "ERROR" first-word other-words)))

(defn critical
  [first-word & other-words]
  (when (not (screened-out? :critical))
    (apply log "CRITICAL" first-word other-words)))
