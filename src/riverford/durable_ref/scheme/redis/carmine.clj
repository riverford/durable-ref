(ns riverford.durable-ref.scheme.redis.carmine
  "Carmine-backed Redis implementation.

  Supports the following reference types:

  - atomic
  - value
  - volatile

  References follow the scheme <reftype>:redis:tcp://<host>:<port>/<dbnumber>/<key>.<format>, e.g.:

  atomic:redis:tcp://localhost:6379/0/atom-ref.edn

  To be able to use Clojure's reference type interfaces you can add the Redis credentials
  using `add-credentials!`, and remove them again when no longer needed with `remove-credentials!`.
  Example, with Redis password \"foobar\":

  `(add-credentials! \"localhost\" 6379 \"foobar\")`
  `(def ref-1 (dref/reference \"atomic:redis:tcp://localhost:6379/0/ref-1.edn\"))`
  `@ref-1 ;=> nil`
  `(reset! ref-1 {:foo #{:a :b}})`
  `@ref-1 ;=> {:foo #{:a :b}}`
  `(swap! ref-1 assoc :bar 42)`
  `@ref-1 ;=> {:foo #{:a :b} :bar 42}`
  "
  (:require
    [clojure.edn :as edn]
    [clojure.string :as str]
    [riverford.durable-ref.core :as dref]
    [taoensso.carmine :as car])
  (:import
    (java.net URI)))

(defonce connections (atom {}))
(defonce credentials (atom {}))

(defn add-credentials!
  [host port password]
  (swap! credentials assoc (str host ":" port) {:password password}))

(defn remove-credentials!
  [host port]
  (swap! credentials dissoc (str host ":" port)))

(defn- uri->connection
  [^URI uri]
  (let [sub-uri (URI. (.getSchemeSpecificPart uri))
        path (.getPath sub-uri)]
    (if-let [connection (get @connections (.getHost sub-uri))]
      connection
      (let [new-connection {:pool {}
                            :spec {:host (.getHost sub-uri)
                                   :port (let [port (.getPort sub-uri)]
                                           (if (pos? port)
                                             port
                                             6379))}}]
        (swap! connections assoc (str (get-in new-connection [:spec :host]) ":" (get-in new-connection [:spec :port])) new-connection)
        new-connection))))

(defn- location
  [^URI uri]
  (let [sub-uri (URI. (.getSchemeSpecificPart uri))
        path (.getPath sub-uri)
        [_ db id] (str/split path #"/")]
    [(edn/read-string db)
     id]))

(defn- get-credentials
  [opts connection]
  (let [c1 (-> opts :scheme :redis :carmine :credentials)
        c2 (get @credentials
                (str (get-in connection [:spec :host])
                     ":"
                     (get-in connection [:spec :port])))]
    (or c1 c2)))

(defmethod dref/write-bytes! "redis"
  [^URI uri bytes opts]
  (let [connection (uri->connection uri)
        credentials (get-credentials opts connection)
        [db k] (location uri)]
    (car/wcar (cond-> connection
                  (:password credentials) (assoc-in [:spec :password] (:password credentials))
                  true (assoc-in [:spec :db] db))
              (car/set k bytes))))

(defmethod dref/read-bytes "redis"
  [^URI uri opts]
  (let [connection (uri->connection uri)
        credentials (get-credentials opts connection)
        [db k] (location uri)
        conn-w-creds (cond-> connection
                         (:password credentials) (assoc-in [:spec :password] (:password credentials))
                         true (assoc-in [:spec :db] db))]
    (car/wcar conn-w-creds
              (car/get k))))

(defmethod dref/delete-bytes! "redis"
  [^URI uri opts]
  (let [connection (uri->connection uri)
        credentials (get-credentials opts connection)
        [db k] (location uri)]
    (car/wcar (cond-> connection
                  (:password credentials) (assoc-in [:spec :password] (:password credentials))
                  true (assoc-in [:spec :db] db))
              (car/del k))))

(defmethod dref/do-atomic-swap! "redis"
  [^URI uri f opts]
  (let [connection (uri->connection uri)
        credentials (get-credentials opts connection)
        [db k] (location uri)
        deserialize (dref/get-deserializer uri)
        serialize (dref/get-serializer uri)
        conn (cond-> connection
                 (:password credentials) (assoc-in [:spec :password] (:password credentials))
                 true (assoc-in [:spec :db] db))]
    (let [[_ [_ res-raw]] (car/atomic conn 100
                                      (car/watch k)
                                      (let [curr-raw (car/with-replies (car/get k))
                                            curr-val (when curr-raw
                                                       (deserialize curr-raw opts))]
                                        (car/multi)
                                        (car/set k (serialize (f curr-val) opts))
                                        (car/get k)))]
      (deserialize res-raw opts))))

