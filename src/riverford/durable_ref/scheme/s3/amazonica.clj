(ns riverford.durable-ref.scheme.s3.amazonica
  (:require [riverford.durable-ref.core :as dref]
            [amazonica.aws.s3 :as s3]
            [clojure.string :as str]
            [clojure.java.io :as io])
  (:import (java.net URI)
           (java.io ByteArrayOutputStream)
           (com.amazonaws AmazonServiceException)))

(defn- mapply
  ([f m]
   (apply f (apply concat m)))
  ([f a & args]
   (apply f a (apply concat (butlast args) (last args)))))

(defn- strip-leading-slash
  [s]
  (if (str/starts-with? s "/")
    (recur (subs s 1))
    s))

(defmethod dref/write-bytes! "s3"
  [^URI uri bytes opts]
  (let [opts (merge
               (-> opts :scheme :s3 :amazonica :shared-opts)
               (-> opts :scheme :s3 :amazonica :write-opts))]
    (mapply s3/put-object
      :bucket-name (.getAuthority uri)
      :key (strip-leading-slash (.getPath uri))
      :metadata (merge {:content-length (count bytes)}
                       (:metadata opts))
      :input-stream (io/input-stream bytes)
      (dissoc opts :metadata))))

(defmethod dref/read-bytes "s3"
  [^URI uri opts]
  (try
    (let [in (:input-stream (mapply s3/get-object
                                    :bucket-name (.getAuthority uri)
                                    :key (strip-leading-slash (.getPath uri))
                                    (merge
                                      (-> opts :scheme :s3 :amazonica :shared-opts)
                                      (-> opts :scheme :s3 :amazonica :read-opts))))
          bao (ByteArrayOutputStream.)]
      (with-open [in in]
        (io/copy in bao))
      (.toByteArray bao))
    (catch AmazonServiceException e
      (when (not= "NoSuchKey" (.getErrorCode e))
        (throw e)))))

(defmethod dref/delete-bytes! "s3"
  [^URI uri opts]
  (try
    (mapply s3/delete-object
            :bucket-name (.getAuthority uri)
            :key (strip-leading-slash (.getPath uri))
            (merge
              (-> opts :scheme :s3 :amazonica :shared-opts)
              (-> opts :scheme :s3 :amazonica :delete-opts)))
    (catch AmazonServiceException e
      (when (not= "NoSuchKey" (.getErrorCode e))
        (throw e)))))