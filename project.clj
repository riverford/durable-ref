(defproject riverford/durable-ref "0.1.6"
  :description "Durable reference types"
  :url "https://github.com/riverford/durable-ref/"
  :license "https://github.com/riverford/durable-ref/blob/master/LICENSE"
  :dependencies [[org.clojure/clojure "1.8.0"]]
  :test-selectors {:default (complement :integration)
                   :integration :integration
                   :all (constantly true)}
  :profiles {:dev {:dependencies [[org.clojure/data.fressian "0.2.1"]
                                  [amazonica "0.3.77"]
                                  [cheshire "5.6.3"]
                                  [com.taoensso/nippy "2.13.0"]
                                  [com.taoensso/carmine "2.16.0"]]}})
