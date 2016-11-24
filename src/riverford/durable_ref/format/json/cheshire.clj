(ns riverford.durable-ref.format.json.cheshire
  (:require [cheshire.core :as cheshire]
            [riverford.durable-ref.core :as dref]
            [clojure.java.io :as io])
  (:import (java.io ByteArrayOutputStream)
           (java.util.zip GZIPOutputStream GZIPInputStream)))

(defmethod dref/serialize "json"
  [obj _ opts]
  (let [bao (ByteArrayOutputStream.)]
    (with-open [w (io/writer bao)]
      (cheshire/generate-stream obj w (-> opts :format :json :cheshire :write-opts)))
    (.toByteArray bao)))

(defmethod dref/deserialize "json"
  [in _ opts]
  (let [read-opts (-> opts :format :json :cheshire :read-opts)]
    (with-open [rdr (io/reader in)]
      (cheshire/parse-stream
        rdr
        (:key-fn read-opts identity)
        (:array-coerce-fn read-opts (constantly []))))))

(defmethod dref/serialize "json.zip"
  [obj _ opts]
  (let [bao (ByteArrayOutputStream.)]
    (with-open [gzipo (GZIPOutputStream. bao)
                w (io/writer gzipo)]
      (cheshire/generate-stream obj w (-> opts :format :json :cheshire :write-opts)))
    (.toByteArray bao)))

(defmethod dref/deserialize "json.zip"
  [in _ opts]
  (let [read-opts (-> opts :format :json :cheshire :read-opts)]
    (with-open [in (io/input-stream in)
                gzipi (GZIPInputStream. in)
                rdr (io/reader gzipi)]
      (cheshire/parse-stream
        rdr
        (:key-fn read-opts identity)
        (:array-coerce-fn read-opts (constantly []))))))