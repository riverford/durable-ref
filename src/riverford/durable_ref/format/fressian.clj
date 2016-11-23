(ns riverford.durable-ref.format.fressian
  (:require [clojure.java.io :as io]
            [clojure.data.fressian :as fress]
            [riverford.durable-ref.core :as dref])
  (:import (java.util.zip GZIPInputStream GZIPOutputStream)
           (java.io ByteArrayOutputStream)))

(defmethod dref/serialize "fress.zip"
  [obj _]
  (let [bao (ByteArrayOutputStream.)]
    (with-open [zipout (GZIPOutputStream. bao)
                w (fress/create-writer zipout)]
      (fress/write-object w obj))
    (.toByteArray bao)))

(defmethod dref/deserialize "fress.zip"
  [in _]
  (with-open [in (io/input-stream in)
              zipin (GZIPInputStream. in)
              rdr (fress/create-reader zipin)]
    (fress/read-object rdr)))

(defmethod dref/serialize "fress"
  [obj _]
  (let [bao (ByteArrayOutputStream.)]
    (with-open [fw (fress/create-writer bao)]
      (fress/write-object fw obj))
    (.toByteArray bao)))

(defmethod dref/deserialize "fress"
  [in _]
  (with-open [in (io/input-stream in)
              fr (fress/create-reader in)]
    (fress/read-object fr)))

(defmethod dref/serialize "fress.zip"
  [obj _]
  (let [bao (ByteArrayOutputStream.)]
    (with-open [gzipo (GZIPOutputStream. bao)
                fw (fress/create-writer gzipo)]
      (fress/write-object fw obj))
    (.toByteArray bao)))

(defmethod dref/deserialize "fress.zip"
  [in _]
  (with-open [in (io/input-stream in)
              gzipi (GZIPInputStream. in)
              fr (fress/create-reader gzipi)]
    (fress/read-object fr)))
