(ns riverford.durable-ref.core
  "Provides a DurableRef and DurableValueRef. Both a derefable reference types
  that refer to clojure data potentially off-heap.

  DurableRef represents a durable mutable cell. Create with `reference` on a valid URI.
  DurableValueRef represents a content-addressed durable immutable cell. Create with `persist!` with a valid base-uri
  and optional format.

  You can refer to a durable ref with the `reference` function,
  that takes a URI.

  The uri scheme should follow like so:
  `dref:mem://foo/bar/baz.edn`
  `dvref:mem://foo/bar/f79975f99908e5387fca3b503d9e9adbefaafc5e.edn.zip`

  Where `mem` is a subscheme that describes how to read and write bytes to a location described by the URI.
  `mem` & `file` are schemes supported by default. See the scheme.* namespaces for extra schemes.

  Extend to new sub schemes via read-bytes, write-bytes! and delete-bytes!.

  Storage format is dispatched from a convention based uri file extension component. (e.g .edn, .edn.zip).
  Extend to new extensions via serialize and deserialize.

  See format.* namespaces for more formats."
  (:refer-clojure :exclude [deref])
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.edn :as edn])
  (:import (java.net URI)
           (java.util.zip GZIPInputStream GZIPOutputStream)
           (java.security MessageDigest DigestInputStream)
           (javax.xml.bind DatatypeConverter)
           (java.io ByteArrayOutputStream PushbackReader ByteArrayInputStream)
           (java.util.concurrent ConcurrentHashMap)
           (clojure.lang IDeref)))

(defmulti serialize (fn [obj format] format))
(defmulti deserialize (fn [in format] format))

(defmethod serialize :default
  [obj format]
  (if-some [idx (some-> format (str/index-of "."))]
    (serialize obj (subs format (inc idx)))
    (throw (IllegalArgumentException. (clojure.core/format "No serialize impl defined for format %s" format)))))

(defmethod deserialize :default
  [in format]
  (if-some [idx (some-> format (str/index-of "."))]
    (deserialize in (subs format (inc idx)))
    (throw (IllegalArgumentException. (clojure.core/format "No deserialize impl defined for format %s" format)))))

(defmethod serialize "edn"
  [obj _]
  (let [bao (ByteArrayOutputStream.)]
    (with-open [w (io/writer bao)]
      (binding [*out* w]
        (pr obj)))
    (.toByteArray bao)))

(defmethod deserialize "edn"
  [in _]
  (with-open [in (io/input-stream in)
              rdr (io/reader in)
              prdr (PushbackReader. rdr)]
    (edn/read prdr)))

(defmethod serialize "edn.zip"
  [obj _]
  (let [bao (ByteArrayOutputStream.)]
    (with-open [zipout (GZIPOutputStream. bao)
                w (io/writer zipout)]
      (binding [*out* w]
        (pr obj)))
    (.toByteArray bao)))

(defmethod deserialize "edn.zip"
  [in _]
  (with-open [in (io/input-stream in)
              zipin (GZIPInputStream. in)
              rdr (io/reader zipin)
              prdr (PushbackReader. rdr)]
    (edn/read prdr)))

(defn- serialize-to
  [obj uri]
  (serialize obj (.getPath ^URI uri)))

(defn- deserialize-from
  [obj uri]
  (deserialize obj (.getPath ^URI uri)))

(defmulti read-bytes (fn [uri opts] (.getScheme ^URI uri)))

(defmulti write-bytes! (fn [uri bytes opts] (.getScheme ^URI uri)))

(defmulti delete-bytes! (fn [uri opts] (.getScheme ^URI uri)))

(defmethod read-bytes :default
  [uri opts]
  (throw (IllegalArgumentException. (format "Not a ref scheme %s" (.getScheme uri)))))

(defmethod write-bytes! :default
  [uri opts bytes]
  (throw (IllegalArgumentException. (format "Not a writable ref scheme %s" (.getScheme uri)))))

(defmethod delete-bytes! :default
  [uri opts]
  (throw (IllegalArgumentException. (format "Not a deletable ref scheme %s" (.getScheme uri)))))

(defprotocol IDurableRef
  (-deref [this opts]))

(deftype DurableRef [^URI uri]
  IDurableRef
  (-deref [this opts]
    (when-some [bytes (read-bytes (.getSchemeSpecificPart uri) opts)]
      (deserialize-from bytes uri)))
  IDeref
  (deref [this]
    (-deref this {}))
  Object
  (equals [this obj]
    (and (instance? DurableRef obj)
         (= uri (.-uri ^DurableRef obj))))
  (hashCode [this]
    (.hashCode uri)))

(defmethod print-method DurableRef
  [obj w]
  (.write w (str "#dref \"" (.-uri ^DurableRef obj) "\"")))

(defn- mutable-ref
  [uri]
  (let [uri (URI. (str/lower-case (str uri)))]
    (assert (= "dref" (.getScheme ^URI uri)) "Invalid URI, scheme must be dref.")
    (->DurableRef uri)))

(defn- sha1
  ([x]
   (let [digest (MessageDigest/getInstance "sha1")]
     (with-open [in (io/input-stream x)
                 din (DigestInputStream. in digest)]
       (while (not= -1 (.read din))))
     (.digest digest))))

(defn- hex-encode
  [bytes]
  (DatatypeConverter/printHexBinary bytes))

(def ^:dynamic *perform-checksum-check*
  true)

(deftype DurableValueRef [^URI uri ^:volatile-mutable _val]
  IDurableRef
  (-deref [this opts]
    (cond
      (= ::nil _val) nil
      (some? _val) _val
      :else (let [sub-uri (URI. (.getSchemeSpecificPart uri))
                  path (.getPath sub-uri)
                  bytes (read-bytes sub-uri opts)]
              (if (nil? bytes)
                (throw (NullPointerException. "DurableValueRef points to nothing. Storage may have been mutated."))
                (if (or (not *perform-checksum-check*)
                        (= (str/lower-case (hex-encode (sha1 bytes)))
                           (some-> (str/split path #"/")
                                   last
                                   (str/split #"\.")
                                   first
                                   str/lower-case)))
                  (let [v (deserialize-from bytes sub-uri)]
                    (set! _val v)
                    v)
                  (throw (IllegalStateException. "DurableValueRef checksum mismatch. Storage may have been mutated.")))))))
  IDeref
  (deref [this]
    (-deref this {}))
  Object
  (equals [this obj]
    (and (instance? DurableValueRef obj)
         (= uri (.-uri ^DurableValueRef obj))))
  (hashCode [this]
    (.hashCode uri)))

(defmethod print-method DurableValueRef
  [obj w]
  (.write w (str "#dvref \"" (.-uri ^DurableValueRef obj) "\"")))

(defn- value-ref
  [uri]
  (let [uri (URI. (str/lower-case (str uri)))]
    (when (not= "dvref" (.getScheme ^URI uri))
      (throw (IllegalArgumentException.
               (format "Invalid URI, scheme must be dvref (got %s)" (.getScheme uri)))))
    (->DurableValueRef uri nil)))

(defn persist!
  "Persists the obj to a unique location (by value) under `base-uri`.
  Returns a DurableValueRef to the object."
  ([base-uri obj]
   (persist! base-uri obj {}))
  ([base-uri obj opts]
   (let [format (name (or (:format opts) "edn"))
         base-uri (URI. (str base-uri))
         scheme (.getScheme base-uri)]
     (when (nil? (get-method serialize format))
       (throw (IllegalArgumentException. (clojure.core/format "No serialize impl for format (%s)." format))))

     (when (nil? (get-method read-bytes scheme))
       (throw (IllegalArgumentException. (clojure.core/format "No read-bytes impl for uri scheme (%s)"
                                                              scheme))))
     (when (nil? (get-method write-bytes! scheme))
       (throw (IllegalArgumentException. (clojure.core/format "No write-bytes! impl for uri scheme (%s)"
                                                              scheme))))
     (let [bytes (serialize obj format)
           sha1 (sha1 bytes)
           base-uri (str base-uri)
           uri (URI.
                 (str/lower-case
                   (str base-uri
                        (when-not (str/ends-with? base-uri "/") "/")
                        (hex-encode sha1)
                        "."
                        format)))]
       (write-bytes! uri bytes (-> opts :scheme-opts (get scheme)))
       (->DurableValueRef (str "dvref:" uri) (if (nil? obj) ::nil obj))))))

(defn reference
  "Returns a durable ref or durable value ref for the URI according to the scheme."
  [uri]
  (let [uri (URI. (str uri))
        scheme (.getScheme uri)
        sub-scheme (.getScheme (URI. (.getSchemeSpecificPart uri)))]

    (when (nil? (get-method read-bytes sub-scheme))
      (throw (IllegalArgumentException. (clojure.core/format "No read-bytes impl for uri scheme (%s)"
                                                             sub-scheme))))

    (when (nil? (get-method write-bytes! sub-scheme))
      (throw (IllegalArgumentException. (clojure.core/format "No write-bytes! impl for uri scheme (%s)"
                                                             sub-scheme))))

    (case scheme
      "dref" (mutable-ref uri)
      "dvref" (value-ref uri)
      (throw (IllegalArgumentException. (format "Invalid reference scheme %s" scheme))))))

(defn uri
  "Returns the URI of the durable ref."
  [dref]
  (condp instance? dref
    DurableRef (.-uri ^DurableRef dref)
    DurableValueRef (.-uri ^DurableValueRef dref)
    (throw (IllegalArgumentException. (format "Cannot take uri of (%s)" (class dref))))))

(defn overwrite!
  "Writes the obj to the durable ref."
  ([dref obj]
   (overwrite! dref obj {}))
  ([dref obj opts]
   (let [dref (reference dref)
         uri (uri dref)
         sub-uri (.getSchemeSpecificPart ^URI uri)
         sub-scheme (.getScheme (URI. ^URI uri))]
     (when (instance? DurableValueRef dref)
       (throw (IllegalArgumentException. "Cannot overwrite a DurableValueRef.")))
     (write-bytes! sub-uri (serialize-to obj sub-uri) (-> opts :scheme-opts (get sub-scheme))))
   nil))

(defn delete!
  "Deletes a reference from storage.

  Accepts anything that can be referenced via `reference`.

  Will accept a DurableValueRef, but be aware that this will invalidate the ref
  and cause subsequent deref calls on the ref to crash."
  ([dref opts]
   (let [dref (reference dref)
         uri (uri dref)
         sub-scheme (.getScheme (URI. (.getSchemeSpecificPart uri)))]
     (delete-bytes! uri (-> opts :scheme-opts (get sub-scheme))))))

(defn deref
  "Attempts to derefence a durable reference and returns a value.
  dref can be a durable ref, durable value ref, or anything accepted by `reference`.

  May throw an error if in the case of a value ref, storage has been mutated. (override with the *perform-checksum-check*
  var), it may also throw in general if storage is unavailable or crashes for whatever reason."
  ([dref]
   (deref dref {}))
  ([dref opts]
   (if (satisfies? IDurableRef dref)
     (let [uri (uri dref)
           sub-scheme (.getScheme (URI. (.getSchemeSpecificPart uri)))]
       (-deref dref (-> opts :scheme-opts (get sub-scheme))))
     (recur (reference dref) opts))))

;; in memory impl

(defonce ^:private mem
  (ConcurrentHashMap.))

(defmethod write-bytes! "mem"
  [^URI uri bytes opts]
  (.put mem [(.getHost uri) (.getPath uri)] bytes))

(defmethod read-bytes "mem"
  [^URI uri opts]
  (get mem [(.getHost uri) (.getPath uri)]))

(defmethod delete-bytes! "mem"
  [^URI uri opts]
  (.remove mem [(.getHost uri) (.getPath uri)]))

;; file system impl

(defmethod write-bytes! "file"
  [^URI uri bytes opts]
  (let [file (io/file uri)]
    (with-open [os (io/output-stream file)]
      (io/copy bytes os))))

(defmethod read-bytes "file"
  [^URI uri opts]
  (let [file (io/file uri)
        os (ByteArrayOutputStream.)]
    (io/copy file os)
    (.toByteArray os)))

(defmethod delete-bytes! "file"
  [^URI uri opts]
  (let [file (io/file uri)]
    (io/delete-file file)))