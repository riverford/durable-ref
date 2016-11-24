(ns riverford.durable-ref.core
  "Provides reference types that refer to clojure data potentially off-heap or remotely.

  e.g
   (let [dref (dref/persist! \"file:///Users/foobar/objects\" 42 {:format \"edn\"})]
     @dref ;;derefable
     (dref/reference (uri dref)) ;; reobtain the reference from a URI
     (dref/deref (uri dref)) ;; alternative deref operator, takes a URI (and additional options)
    )

  Reference identity is a URI, the scheme of which determines the type.
  e.g
  volatile: mutable, non-cached
  value: immutable, cached & interned while references exist.

  The subscheme determines the storage implementation
  e.g
  file: File system storage
  mem: Transient memory storage

  Further storages can be found in the `scheme` package.
  Extend to new schemes via the multimethods `read-bytes` `write-bytes!` and `delete-bytes!.

  The file extension of the uri determines the storage format.
  e.g
  edn
  edn.zip

  Further formats can be found inthe `format` package.
  Extend to new formats via the multimethods `serialize` and `deserialize`.

  You can deref data with a URI, (or string uri), or a reference object obtained with `reference`.

  Example URI's:
  `volatile:mem://foo/bar/baz.edn`
  `value:file:///Users/foobar/objects/f79975f99908e5387fca3b503d9e9adbefaafc5e.edn.zip`"
  (:refer-clojure :exclude [deref])
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.edn :as edn])
  (:import (java.net URI)
           (java.util.zip GZIPInputStream GZIPOutputStream)
           (java.security MessageDigest DigestInputStream)
           (javax.xml.bind DatatypeConverter)
           (java.io ByteArrayOutputStream PushbackReader)
           (java.util.concurrent ConcurrentHashMap)
           (clojure.lang IDeref)
           (java.util WeakHashMap)
           (java.lang.ref WeakReference)))

(comment
  (set! *warn-on-reflection* true))

(defmulti serialize (fn [obj format opts] format))
(defmulti deserialize (fn [in format opts] format))

(defn get-serializer
  [^URI uri]
  (let [sub-uri (URI. (.getSchemeSpecificPart uri))
        path (.getPath sub-uri)
        m (dissoc (methods serialize) :default)]
    (loop [s path]
      (if-some [idx (some-> s (str/index-of "."))]
        (let [s (subs s (inc idx))]
          (or (get m s)
              (recur s)))
        (throw (IllegalArgumentException. (clojure.core/format "No serialize impl defined for format %s" s)))))))

(defn get-deserializer
  [^URI uri]
  (let [sub-uri (URI. (.getSchemeSpecificPart uri))
        path (.getPath sub-uri)
        m (dissoc (methods deserialize) :default)]
    (loop [s path]
      (if-some [idx (some-> s (str/index-of "."))]
        (let [s (subs s (inc idx))]
          (or (get m s)
              (recur s)))
        (throw (IllegalArgumentException. (clojure.core/format "No deserialize impl defined for format %s" s)))))))

(defmethod serialize :default
  [obj format opts]
  (if-some [idx (some-> format (str/index-of "."))]
    (serialize obj (subs format (inc idx)) opts)
    (throw (IllegalArgumentException. (clojure.core/format "No serialize impl defined for format %s" format)))))

(defmethod deserialize :default
  [in format opts]
  (if-some [idx (some-> format (str/index-of "."))]
    (deserialize in (subs format (inc idx)) opts)
    (throw (IllegalArgumentException. (clojure.core/format "No deserialize impl defined for format %s" format)))))

(defmethod serialize "edn"
  [obj _ opts]
  (let [bao (ByteArrayOutputStream.)]
    (with-open [w (io/writer bao)]
      (binding [*out* w]
        (pr obj)))
    (.toByteArray bao)))

(defmethod deserialize "edn"
  [in _ opts]
  (with-open [in (io/input-stream in)
              rdr (io/reader in)
              prdr (PushbackReader. rdr)]
    (edn/read prdr)))

(defmethod serialize "edn.zip"
  [obj _ opts]
  (let [bao (ByteArrayOutputStream.)]
    (with-open [zipout (GZIPOutputStream. bao)
                w (io/writer zipout)]
      (binding [*out* w]
        (pr obj)))
    (.toByteArray bao)))

(defmethod deserialize "edn.zip"
  [in _ opts]
  (with-open [in (io/input-stream in)
              zipin (GZIPInputStream. in)
              rdr (io/reader zipin)
              prdr (PushbackReader. rdr)]
    (edn/read prdr)))

(defn- serialize-to
  [obj uri opts]
  (serialize obj (.getPath ^URI uri) opts))

(defn- deserialize-from
  [obj uri opts]
  (deserialize obj (.getPath ^URI uri) opts))

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
  (-deref [this opts])
  (-props [this]))

(deftype DurableVolatileRef [^URI uri]
  IDurableRef
  (-deref [this opts]
    (when-some [bytes (read-bytes (.getSchemeSpecificPart uri) opts)]
      (deserialize-from bytes uri opts)))
  (-props [this]
    {:uri uri
     :read-only? false})
  IDeref
  (deref [this]
    (-deref this {}))
  Object
  (equals [this obj]
    (and (instance? DurableVolatileRef obj)
         (= uri (.-uri ^DurableVolatileRef obj))))
  (hashCode [this]
    (.hashCode uri))
  (toString [this]
    (str uri)))

(defn- volatile-ref
  [uri]
  (let [uri (URI. (str/lower-case (str uri)))]
    (assert (= "volatile" (.getScheme ^URI uri)) "Invalid URI, scheme must be volatile.")
    (->DurableVolatileRef uri)))

(defn- hash-identity
  ([x]
   (let [digest (MessageDigest/getInstance "sha1")]
     (with-open [in (io/input-stream x)
                 din (DigestInputStream. in digest)]
       (while (not= -1 (.read din))))
     (.digest digest))))

(defn- hex-encode
  [bytes]
  (DatatypeConverter/printHexBinary bytes))

(def ^:dynamic *verify-hash-identity*
  true)

(deftype DurableValueRef [uri ^:volatile-mutable _val]
  IDurableRef
  (-deref [this opts]
    (cond
      (= ::nil _val) nil
      (some? _val) _val
      :else (locking this
              (let [sub-uri (URI. (.getSchemeSpecificPart ^URI uri))
                    path (.getPath sub-uri)
                    bytes (read-bytes sub-uri opts)]
                (if (nil? bytes)
                  (throw (NullPointerException. "DurableValueRef points to nothing. Storage may have been mutated."))
                  (if (or (not *verify-hash-identity*)
                          (= (str/lower-case (hex-encode (hash-identity bytes)))
                             (some-> (str/split path #"/")
                                     last
                                     (str/split #"\.")
                                     first
                                     str/lower-case)))
                    (let [v (deserialize-from bytes sub-uri opts)]
                      (set! _val v)
                      v)
                    (throw (IllegalStateException. "DurableValueRef checksum mismatch. Storage may have been mutated."))))))))
  (-props [this]
    {:uri uri
     :read-only? true})
  IDeref
  (deref [this]
    (-deref this {}))
  Object
  (equals [this obj]
    (and (instance? DurableValueRef obj)
         (= uri (.-uri ^DurableValueRef obj))))
  (hashCode [this]
    (.hashCode uri))
  (toString [this]
    (str uri)))

(defn- value-ref
  [uri]
  (let [uri (URI. (str/lower-case (str uri)))]
    (when (not= "value" (.getScheme ^URI uri))
      (throw (IllegalArgumentException.
               (format "Invalid URI, scheme must be value (got %s)" (.getScheme uri)))))
    (->DurableValueRef uri nil)))

(deftype DurableReadonlyRef [uri]
  IDurableRef
  (-deref [this opts]
    (when-some [bytes (read-bytes uri opts)]
      (deserialize-from bytes uri opts)))
  (-props [this]
    {:uri uri
     :read-only? true})
  IDeref
  (deref [this]
    (-deref this {}))
  Object
  (equals [this obj]
    (and (instance? DurableReadonlyRef obj)
         (= uri (.-uri ^DurableReadonlyRef obj))))
  (hashCode [this]
    (.hashCode uri))
  (toString [this]
    (str uri)))

(defmethod print-method riverford.durable_ref.core.IDurableRef
  [o w]
  (.write w (str "#object [" (.getName (class o)) " \"" o "\"]")))

(prefer-method print-method riverford.durable_ref.core.IDurableRef IDeref)

(def ^:private intern-pool
  (WeakHashMap.))

(def ^:private interned?
  (let [pool ^WeakHashMap intern-pool]
    (fn [obj]
      (locking pool
        (.containsKey pool obj)))))

(def ^:private intern-ref
  (let [pool ^WeakHashMap intern-pool]
    (fn [obj]
      (locking pool
        (loop [ret nil]
          (if (some? ret)
            ret
            (let [wref (get pool obj)]
              (if (nil? wref)
                (do (.put pool obj (WeakReference. obj))
                    obj)
                (recur (.get ^WeakReference wref))))))))))

(defn reference
  "Returns a ref for the URI according to the scheme.
   e.g value:mem:/foo/bar."
  [uri]
  (let [uri (URI. (str uri))
        scheme (.getScheme uri)
        sub-scheme (.getScheme (URI. (.getSchemeSpecificPart uri)))]
    (if (nil? sub-scheme)
      (do
        (when (nil? (get-method read-bytes scheme))
          (throw (IllegalArgumentException. (clojure.core/format "No read-bytes impl for uri scheme (%s)"
                                                                 scheme))))

        (when (nil? (get-method write-bytes! scheme))
          (throw (IllegalArgumentException. (clojure.core/format "No write-bytes! impl for uri scheme (%s)"
                                                                 scheme))))

        (->DurableReadonlyRef uri))
      (do
        (when (nil? (get-method read-bytes sub-scheme))
          (throw (IllegalArgumentException. (clojure.core/format "No read-bytes impl for uri scheme (%s)"
                                                                 sub-scheme))))

        (when (nil? (get-method write-bytes! sub-scheme))
          (throw (IllegalArgumentException. (clojure.core/format "No write-bytes! impl for uri scheme (%s)"
                                                                 sub-scheme))))

        (case scheme
          "volatile" (volatile-ref uri)
          "value" (intern-ref (value-ref uri))

          (throw (IllegalArgumentException. (format "Invalid reference scheme %s" scheme))))))))

(defn read-only?
  [dref]
  (if (satisfies? IDurableRef dref)
    (:read-only? (-props dref))
    (recur (reference dref))))

(defn uri
  "Returns the URI of the ref."
  [dref]
  (if (satisfies? IDurableRef dref)
    (:uri (-props dref))
    (recur (reference dref))))

(defn overwrite!
  "Writes the obj to the mutable ref."
  ([dref obj]
   (overwrite! dref obj {}))
  ([dref obj opts]
   (if (satisfies? IDurableRef dref)
     (do
       (when (read-only? dref)
         (throw (IllegalArgumentException. "Cannot overwrite a readonly ref.")))
       (let [uri (uri dref)
             sub-uri (.getSchemeSpecificPart ^URI uri)]
         (write-bytes! sub-uri (serialize-to obj sub-uri opts) opts))
       nil)
     (recur (reference dref) obj opts))))

(defn delete!
  "Deletes a reference from storage.

  Accepts anything (apart from readonly refs) that can be referenced via `reference`."
  ([dref]
   (delete! dref {}))
  ([dref opts]
   (if (satisfies? IDurableRef dref)
     (do
       (when (read-only? dref)
         (throw (IllegalArgumentException. "Cannot delete a readonly ref")))
       (let [uri (uri dref)]
         (delete-bytes! uri opts)))
     (recur (reference dref) opts))))

(defn persist!
  "Persists the obj to a unique location (by value) under `base-uri`.
  Returns a DurableValueRef to the object."
  ([base-uri obj]
   (persist! base-uri obj {}))
  ([base-uri obj opts]
   (let [base-uri (str base-uri)
         format (name (or (:format opts) "edn"))
         bytes (serialize obj format opts)
         sha1 (hash-identity bytes)
         uri (URI.
               (str/lower-case
                 (str base-uri
                      (when-not (str/ends-with? base-uri "/") "/")
                      (hex-encode sha1)
                      "."
                      format)))
         full-uri (URI. (str "value:" uri))
         deserialized (deserialize bytes format opts)]
     (when (not (interned? (value-ref full-uri)))
       (write-bytes! uri bytes opts))
     (intern-ref
       (->DurableValueRef full-uri (if (nil? deserialized) ::nil deserialized))))))

(defn deref
  "Attempts to derefence a durable reference and returns a value.
  dref can be a anything accepted by `reference`.

  May throw an error if in the case of a value ref, storage has been mutated.
   (override with the *verify-hash-identity* var), it may also throw in general if storage is unavailable or crashes for whatever reason."
  ([dref]
   (deref dref {}))
  ([dref opts]
   (if (satisfies? IDurableRef dref)
     (-deref dref opts)
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
    (io/copy bytes file)))

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