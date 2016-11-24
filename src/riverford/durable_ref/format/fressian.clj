(ns riverford.durable-ref.format.fressian
  (:require [clojure.java.io :as io]
            [clojure.data.fressian :as fress]
            [riverford.durable-ref.core :as dref])
  (:import (java.util.zip GZIPInputStream GZIPOutputStream)
           (java.io ByteArrayOutputStream)))

(defn- mapply
  ([f m]        (apply f (apply concat m)))
  ([f a & args] (apply f a (apply concat (butlast args) (last args)))))

(defmethod dref/serialize "fressian"
  [obj _ opts]
  (let [bao (ByteArrayOutputStream.)]
    (with-open [fw (mapply fress/create-writer bao (:fressian opts))]
      (fress/write-object fw obj))
    (.toByteArray bao)))

(defmethod dref/deserialize "fressian"
  [in _ opts]
  (with-open [in (io/input-stream in)
              fr (mapply fress/create-reader in (:fressian opts))]
    (fress/read-object fr)))

(defmethod dref/serialize "fressian.zip"
  [obj _ opts]
  (let [bao (ByteArrayOutputStream.)]
    (with-open [gzipo (GZIPOutputStream. bao)
                fw (mapply fress/create-writer gzipo (:fressian opts))]
      (fress/write-object fw obj))
    (.toByteArray bao)))

(defmethod dref/deserialize "fressian.zip"
  [in _ opts]
  (with-open [in (io/input-stream in)
              gzipi (GZIPInputStream. in)
              fr (mapply fress/create-reader gzipi (:fressian opts))]
    (fress/read-object fr)))