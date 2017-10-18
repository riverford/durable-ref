(ns riverford.durable-ref.scheme.redis.carmine
  "Carmine-backed Redis implementation.

  Supports the following reference types:

  - atomic
  - value
  - volatile

  References follow the scheme <reftype>:redis:tcp://<host>:<port>/<dbnumber>/<key>.<format>, e.g.:

  atomic:redis:tcp://localhost:6379/0/atom-ref.edn

  Keys can be namespaced further, e.g.

  atomic:redis:tcp://localhost:6379/0/atoms/a1-ref.edn

  This results in the top-level atoms key in Redis being a hash map with a single
  entry a1-ref.edn. A second ref,

  atomic:redis:tcp://localhost:6379/0/atoms/a2-ref.edn

  inserts another entry in the top-level atoms key in Redis, and so on."
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
        [_ db id-or-ns ?id] (str/split path #"/")]
    [(edn/read-string db)
     (when ?id id-or-ns)
     (or ?id id-or-ns)]))

(defn- get-credentials
  [opts connection]
  (let [c1 (:credentials opts)
        c2 (get @credentials
                (str (get-in connection [:spec :host])
                     ":"
                     (get-in connection [:spec :port])))]
    (or c1 c2)))

(defmethod dref/write-bytes! "redis"
  [^URI uri bytes opts]
  (let [connection (uri->connection uri)
        credentials (get-credentials opts connection)
        [db ?f k] (location uri)]
    (car/wcar (-> connection
                  (assoc-in [:spec :password] (or (:password credentials) ""))
                  (assoc-in [:spec :db] db))
              (if ?f
                (car/hset ?f k bytes)
                (car/set k bytes)))))

(defmethod dref/read-bytes "redis"
  [^URI uri opts]
  (let [connection (uri->connection uri)
        credentials (get-credentials opts connection)
        [db ?f k] (location uri)
        conn-w-creds (-> connection
                         (assoc-in [:spec :password] (or (:password credentials) ""))
                         (assoc-in [:spec :db] db))]
    (car/wcar conn-w-creds
              (if ?f
                (car/hget ?f k)
                (car/get k)))))

(defmethod dref/delete-bytes! "redis"
  [^URI uri opts]
  (let [connection (uri->connection uri)
        credentials (get-credentials opts connection)
        [db ?f k] (location uri)]
    (car/wcar (-> connection
                  (assoc-in [:spec :password] (or (:password credentials) ""))
                  (assoc-in [:spec :db] db))
              (if ?f
                (car/hdel ?f k)
                (car/del k)))))

(defmethod dref/do-atomic-swap! "redis"
  [^URI uri f opts]
  (let [connection (uri->connection uri)
        credentials (get-credentials opts connection)
        [db ?f k] (location uri)
        deserialize (dref/get-deserializer uri)
        serialize (dref/get-serializer uri)
        conn (-> connection
                 (assoc-in [:spec :password] (or (:password credentials) ""))
                 (assoc-in [:spec :db] db))]
    (let [[_ [_ res-raw]] (car/atomic conn 100
                                      (car/watch ?f)
                                      (let [curr-raw (if ?f
                                                       (car/with-replies (car/hget ?f k))
                                                       (car/with-replies (car/get k)))
                                            curr-val (when curr-raw
                                                       (deserialize curr-raw opts))]
                                        (car/multi)
                                        (if ?f
                                          (do
                                            (car/hset ?f k (serialize (f curr-val) opts))
                                            (car/hget ?f k))
                                          (do
                                            (car/set k (serialize (f curr-val) opts))
                                            (car/get k)))))]
      (deserialize res-raw opts))))

