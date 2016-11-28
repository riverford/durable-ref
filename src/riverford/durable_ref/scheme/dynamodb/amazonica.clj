(ns riverford.durable-ref.scheme.dynamodb.amazonica
  (:require [amazonica.aws.dynamodbv2 :as dynamodb]
            [riverford.durable-ref.core :as dref]
            [clojure.string :as str])
  (:import (java.net URI)
           (com.amazonaws.services.dynamodbv2.model ConditionalCheckFailedException)))

(defn- mapply
  ([f m]
   (apply f (apply concat m)))
  ([f a & args]
   (apply f a (apply concat (butlast args) (last args)))))

(defn- location
  [^URI uri]
  (let [sub-uri (URI. (.getSchemeSpecificPart uri))
        path (.getPath sub-uri)
        [_ table id] (str/split path #"/")]
    {:endpoint (str (.getScheme sub-uri) "://" (.getHost sub-uri)
                    (let [port (.getPort sub-uri)]
                      (when (pos? port)
                        (str ":" port))))
     :table table
     :id id}))

(defmethod dref/write-bytes! "dynamodb"
  [^URI uri bytes opts]
  (let [{:keys [table id endpoint]} (location uri)]
    (mapply dynamodb/put-item
      (merge {:endpoint endpoint}
             (-> opts :scheme :dynamodb :amazonica :creds))
      :table-name table
      :item {:id id
             :data bytes}
      (-> opts :scheme :dynamodb :amazonica :write-opts))))

(defmethod dref/read-bytes "dynamodb"
  [^URI uri opts]
  (let [{:keys [table id endpoint]} (location uri)]
    (some-> (mapply dynamodb/get-item
              (merge {:endpoint endpoint}
                     (-> opts :scheme :dynamodb :amazonica :creds))
              :table-name table
              :key {:id {:s id}}
              (-> opts :scheme :dynamodb :amazonica :read-opts))
            :item
            :data
            .array)))

(defmethod dref/delete-bytes! "dynamodb"
  [^URI uri opts]
  (let [{:keys [table id endpoint]} (location uri)]
    (mapply dynamodb/delete-item
      (merge {:endpoint endpoint}
             (-> opts :scheme :dynamodb :amazonica :creds))
      :table-name table
      :key {:id {:s id}}
      (-> opts :scheme :dynamodb :amazonica :delete-opts))))

(defmethod dref/do-atomic-swap! "dynamodb"
  [^URI uri f opts]
  (let [deserialize (dref/get-deserializer uri)
        serialize (dref/get-serializer uri)
        {:keys [table id endpoint]} (location uri)]
    (trampoline
      (fn ! [n]
        (let [ret (mapply dynamodb/get-item
                    (merge {:endpoint endpoint}
                           (-> opts :scheme :dynamodb :amazonica :creds))
                    :table-name table
                    :key {:id {:s id}}
                    :consistent-read true
                    (-> opts :scheme :dynamodb :amazonica :read-opts))
              item (:item ret)
              obj (some-> item :data .array (deserialize opts))
              newobj (f obj)
              version (long (:version item -1))]
          (if (= obj newobj)
            newobj
            (try
              (let [data (serialize newobj opts)]
                (mapply dynamodb/put-item
                  (merge {:endpoint endpoint}
                         (-> opts :scheme :dynamodb :amazonica :creds))
                  :table-name table
                  :item {:id id
                         :data data
                         :version (inc version)}
                  :condition-expression "attribute_not_exists(version) OR (version = :version)"
                  :expression-attribute-values {":version" {:n version}}
                  (-> opts :scheme :dynamodb :amazonica :write-opts)))
              newobj
              (catch ConditionalCheckFailedException e
                (when-some [cas-backoff-fn (-> opts :scheme :dynamodb :amazonica :cas-back-off-fn)]
                  (cas-backoff-fn uri (inc n)))
                (partial ! (inc n)))))))
      0)))