(defproject valve "0.1.0-SNAPSHOT"
  :description "VALVE is A Lightweight Validation Engine"
  :url "https://github.com/ontodev/valve.clj"
  :license {:name "BSD 3-Clause License"
            :url "https://opensource.org/licenses/BSD-3-Clause"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/tools.cli "1.0.206"]
                 [org.clojure/data.csv "1.0.0"]
                 [instaparse "1.4.10"]]
  :main ^:skip-aot valve.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
