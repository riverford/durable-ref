(ns riverford.durable-ref.scheme.amazonica.s3
  (:require [riverford.durable-ref.core :as dref]
            [amazonica.aws.s3 :as s3]
            [clojure.string :as str]
            [clojure.java.io :as io])
  (:import (java.net URI)
           (java.io ByteArrayOutputStream)))

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
  (mapply s3/put-object
    :bucket-name (.getAuthority uri)
    :key (strip-leading-slash (.getPath uri))
    :metadata {:content-length (count bytes)}
    :input-stream (io/input-stream bytes)
    (merge
      (-> opts :amazonica :s3 :shared-opts)
      (-> opts :amazonica :s3 :write-opts))))

(defmethod dref/read-bytes "s3"
  [^URI uri opts]
  (let [in (:input-stream (mapply s3/get-object
                            :bucket-name (.getAuthority uri)
                            :key (strip-leading-slash (.getPath uri))
                            (merge
                              (-> opts :amazonica :s3 :shared-opts)
                              (-> opts :amazonica :s3 :read-opts))))
        bao (ByteArrayOutputStream.)]
    (with-open [in in]
      (io/copy in bao))
    (.toByteArray bao)))

(defmethod dref/delete-bytes! "s3"
  [^URI uri opts]
  (mapply s3/delete-object
    :bucket-name (.getAuthority uri)
    :key (strip-leading-slash (.getPath uri))
    (merge
      (-> opts :amazonica :s3 :shared-opts)
      (-> opts :amazonica :s3 :delete-opts))))