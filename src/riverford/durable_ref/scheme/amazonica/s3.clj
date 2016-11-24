(ns riverford.durable-ref.scheme.amazonica.s3
  (:require [riverford.durable-ref.core :as dref]
            [amazonica.aws.s3 :as s3]
            [clojure.string :as str]
            [clojure.java.io :as io])
  (:import (java.net URI)
           (java.io ByteArrayOutputStream)))

(defn- strip-leading-slash
  [s]
  (if (str/starts-with? s "/")
    (recur (subs s 1))
    s))

(defmethod dref/write-bytes! "s3"
  [^URI uri bytes opts]
  (s3/put-object
    :bucket-name (.getAuthority uri)
    :key (strip-leading-slash (.getPath uri))
    :metadata {:content-length (count bytes)}
    :input-stream (io/input-stream bytes)))

(defmethod dref/read-bytes "s3"
  [^URI uri opts]
  (let [in (:input-stream (s3/get-object
                            :bucket-name (.getAuthority uri)
                            :key (strip-leading-slash (.getPath uri))))
        bao (ByteArrayOutputStream.)]
    (with-open [in in]
      (io/copy in bao))
    (.toByteArray bao)))

(defmethod dref/delete-bytes! "s3"
  [^URI uri opts]
  (s3/delete-object
    :bucket-name (.getAuthority uri)
    :key (strip-leading-slash (.getPath uri))))