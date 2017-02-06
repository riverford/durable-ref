(ns riverford.durable-ref.format.nippy
  (:require [clojure.java.io :as io]
            [taoensso.nippy :as nippy]
            [riverford.durable-ref.core :as dref])
  (:import (java.io ByteArrayOutputStream)))

(defmethod dref/serialize "nippy"
  [obj _ opts]
  (nippy/freeze
    obj (-> opts :format :nippy :write-opts)))

(defmethod dref/deserialize "nippy"
  [in _ opts]
  (let [out (ByteArrayOutputStream.)]
    (with-open [in (io/input-stream in)]
      (io/copy in out))
    (nippy/thaw
      (.toByteArray out)
      (-> opts :format :nippy :read-opts))))