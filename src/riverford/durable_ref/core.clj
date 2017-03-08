(ns riverford.durable-ref.core
  "Provides reference types that refer to clojure data potentially off-heap or remotely.

  e.g
   (let [dref (dref/persist \"file:///Users/foobar/objects\" 42 {:as \"edn\"})]
     @dref ;; derefable
     (dref/reference (uri dref)) ;; reobtain the reference from a URI
     (dref/value (uri dref)) ;; alternative deref operator, takes a URI (and additional options)
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

  You can deref via `value` a URI, (or string uri), or a reference object obtained with `reference`.

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
           (clojure.lang IDeref IAtom IObj IPending)
           (java.util WeakHashMap)
           (java.lang.ref WeakReference)))

(comment
  (set! *warn-on-reflection* true))

(defmulti serialize (fn [obj format opts] format))
(defmulti deserialize (fn [in format opts] format))

(defn get-serializer
  [uri]
  (let [uri (str uri)
        s (subs uri (str/last-index-of (str uri) "/"))
        m (dissoc (methods serialize) :default)]
    (loop [s s]
      (if-some [idx (some-> s (str/index-of "."))]
        (let [s (subs s (inc idx))]
          (or (when-some [f (get m s)]
                (fn [obj opts]
                  (f obj s opts)))
              (recur s)))
        (throw (IllegalArgumentException. (clojure.core/format "No serialize impl defined for format %s" s)))))))

(defn get-deserializer
  [uri]
  (let [uri (str uri)
        s (subs uri (str/last-index-of (str uri) "/"))
        m (dissoc (methods deserialize) :default)]
    (loop [s s]
      (if-some [idx (some-> s (str/index-of "."))]
        (let [s (subs s (inc idx))]
          (or (when-some [f (get m s)]
                (fn [obj opts]
                  (f obj s opts)))
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
  ((get-serializer uri) obj opts))

(defn- deserialize-from
  [bytes uri opts]
  ((get-deserializer uri) bytes opts))

(defmulti read-bytes (fn [uri opts] (.getScheme ^URI uri)))

(defmulti write-bytes! (fn [uri bytes opts] (.getScheme ^URI uri)))

(defmulti delete-bytes! (fn [uri opts] (.getScheme ^URI uri)))

(defmulti do-atomic-swap! (fn [uri f opts] (.getScheme ^URI uri)))

(defprotocol IDurableRef
  (-deref [this opts])
  (-props [this]))

(defprotocol IDurableCachedRef
  (-evict! [this]))

(deftype DurableVolatileRef [^URI uri]
  IDurableRef
  (-deref [this opts]
    (let [sub-uri (URI. (.getSchemeSpecificPart uri))]
      (when-some [bytes (read-bytes sub-uri opts)]
        (deserialize-from bytes sub-uri opts))))
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

(declare atomic-swap!)

(deftype DurableAtomicRef [^URI uri]
  IDurableRef
  (-deref [this opts]
    (let [sub-uri (URI. (.getSchemeSpecificPart uri))]
      (when-some [bytes (read-bytes sub-uri (assoc opts :consistent? true))]
        (deserialize-from bytes sub-uri opts))))
  (-props [this]
    {:uri uri
     :atomic? true
     :read-only? false})
  IDeref
  (deref [this]
    (-deref this {}))
  IAtom
  (swap [this f]
    (atomic-swap! this f))
  (swap [this f x]
    (swap! this #(f % x)))
  (swap [this f x y]
    (swap! this #(f % x y)))
  (swap [this f x y args]
    (swap! this #(apply f % x y args)))
  (compareAndSet [this x y]
    (throw (IllegalArgumentException. "Cannot compare and set dref directly, use swap!")))
  (reset [this o]
    (write-bytes! (URI. (.getSchemeSpecificPart uri))
                  (serialize-to o uri {})
                  {})
    o)
  Object
  (equals [this obj]
    (and (instance? DurableAtomicRef obj)
         (= uri (.-uri ^DurableAtomicRef obj))))
  (hashCode [this]
    (.hashCode uri))
  (toString [this]
    (str uri)))

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

(deftype DurableValueRef [uri ^:volatile-mutable valref]
  IDurableRef
  (-deref [this opts]
    (cond
      (some? valref) @valref
      :else (locking this
              (let [sub-uri (URI. (.getSchemeSpecificPart ^URI uri))
                    bytes (read-bytes sub-uri opts)]
                (if (nil? bytes)
                  (throw (NullPointerException. "DurableValueRef points to nothing. Storage may have been mutated."))
                  (if (or (not *verify-hash-identity*)
                          (str/includes?
                            (str uri)
                            (str/lower-case (hex-encode (hash-identity bytes)))))
                    (let [v (deserialize-from bytes sub-uri opts)
                          v' (if (instance? IObj v)
                               (with-meta v {::origin this})
                               v)]
                      (set! valref (volatile! v'))
                      v')
                    (throw (IllegalStateException. "DurableValueRef checksum mismatch. Storage may have been mutated."))))))))
  (-props [this]
    {:uri uri
     :realized? (some? valref)
     :read-only? true})
  IDurableCachedRef
  (-evict! [this]
    (set! valref nil))
  IDeref
  (deref [this]
    (-deref this {}))
  IPending
  (isRealized [this]
    (some? valref))
  Object
  (equals [this obj]
    (and (instance? DurableValueRef obj)
         (= uri (.-uri ^DurableValueRef obj))))
  (hashCode [this]
    (.hashCode uri))
  (toString [this]
    (str uri)))

(defn- origin
  [x]
  (::origin (meta x)))

(defn existing-ref
  "If an existing, realized reference can be be found in memory for an identical object, it will be returned."
  [obj]
  (when-some [dref (origin obj)]
    (when (and (instance? DurableValueRef dref)
               (realized? dref)
               ;; we cannot use opts here, we don't know how it was saved.
               (identical? @dref obj))
      dref)))

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
  (.write w (str "#object ["
                 (.getName (class o)) " "
                 (format "0x%x" (System/identityHashCode o))
                 " \"" o "\"]")))

(prefer-method print-method riverford.durable_ref.core.IDurableRef IDeref)

(defn uri
  "Returns the URI of the ref."
  [dref]
  (cond
    (satisfies? IDurableRef dref) (:uri (-props dref))
    (instance? URI dref) dref
    (instance? String dref) (URI. dref)
    :else (throw (IllegalArgumentException. (format "Cannot return uri of %s" (class dref))))))

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
          "volatile" (->DurableVolatileRef uri)
          "value" (intern-ref (->DurableValueRef uri nil))
          "atomic" (->DurableAtomicRef uri)

          (throw (IllegalArgumentException. (format "Invalid reference scheme %s" scheme))))))))

(defn read-only?
  [dref]
  (if (satisfies? IDurableRef dref)
    (:read-only? (-props dref))
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
             sub-uri (URI. (.getSchemeSpecificPart ^URI uri))]
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
       (let [uri (uri dref)
             sub-uri (URI. (.getSchemeSpecificPart uri))]
         (delete-bytes! sub-uri opts)))
     (recur (reference dref) opts))))

(defn persist
  "Persists the obj to a unique location (by value) under `base-uri`.
  Returns a DurableValueRef to the object.

  The format of the returned ref will be determined by the `:as` option (edn by default)."
  ([base-uri obj]
   (persist base-uri obj {}))
  ([base-uri obj opts]
   (let [base-uri (str base-uri)
         format
         ;; backwards compat hack, :format is also used to specify :format opts. Prefer the :as kw.
         (let [f (:format opts)]
           (if (or (string? f) (keyword? f))
             (name f)
             (let [fmt (or (:as opts) "edn")]
               (name fmt))))
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
     (when (not (interned? (->DurableValueRef full-uri nil)))
       (write-bytes! uri bytes opts))
     (intern-ref
       (let [vbox (volatile! nil)
             r (->DurableValueRef full-uri vbox)
             v (if (instance? IObj deserialized)
                 (with-meta deserialized {::origin r})
                 deserialized)]
         (vreset! vbox v)
         r)))))

(defn value
  "Attempts to derefence a durable reference and returns a value.
  dref can be a anything accepted by `reference`.

  May throw an error if in the case of a value ref, storage has been mutated.
  (override with the *verify-hash-identity* var), it may also throw in general if storage is unavailable or crashes for whatever reason."
  ([dref]
   (value dref {}))
  ([dref opts]
   (if (satisfies? IDurableRef dref)
     (-deref dref opts)
     (recur (reference dref) opts))))

(defn evict!
  "Forceably removes any cached value from the ref if present."
  [dref]
  (if (satisfies? IDurableRef dref)
    (when (satisfies? IDurableCachedRef dref)
      (-evict! dref))
    (evict! (reference dref))))

(defn atomic-swap!
  "Applies `f` to the value held by the ref atomically."
  ([dref f]
   (atomic-swap! dref f {}))
  ([dref f opts]
   (let [dref (reference dref)]
     (when (read-only? dref)
       (throw (IllegalArgumentException. "Cannot swap readonly ref")))
     (when-not (:atomic? (-props dref))
       (throw (IllegalArgumentException. "Cannot swap non-atomic ref")))
     (let [uri (uri dref)
           sub-uri (URI. (.getSchemeSpecificPart uri))]
       (do-atomic-swap! sub-uri f opts)))))

;; in memory impl

(defonce ^:private mem
  (atom {}))

(defn- mematom
  [k]
  (or (get @mem k)
      (-> (swap! mem update k (fn [a] (if a a (atom nil))))
          (get k))))

(defmethod write-bytes! "mem"
  [^URI uri bytes opts]
  (reset! (mematom [(.getHost uri) (.getPath uri)]) bytes)
  nil)

(defmethod read-bytes "mem"
  [^URI uri opts]
  (some-> @mem (get [(.getHost uri) (.getPath uri)]) clojure.core/deref))

(defmethod delete-bytes! "mem"
  [^URI uri opts]
  (swap! mem dissoc [(.getHost uri) (.getPath uri)])
  nil)

(defmethod do-atomic-swap! "mem"
  [^URI uri f opts]
  (let [serialize (get-serializer uri)
        deserialize (get-deserializer uri)]
    (some-> (swap! (mematom [(.getHost uri) (.getPath uri)])
                   (fn [bytes]
                     (-> (when bytes
                           (deserialize bytes opts))
                         f
                         (serialize opts))))
            (deserialize opts))))

;; file system impl

(defmethod write-bytes! "file"
  [^URI uri bytes opts]
  (let [file (io/file uri)]
    (.mkdirs (io/file (.getParent file)))
    (io/copy bytes file)))

(defmethod read-bytes "file"
  [^URI uri opts]
  (let [file (io/file uri)
        os (ByteArrayOutputStream.)]
    (when (.exists file)
      (io/copy file os)
      (.toByteArray os))))

(defmethod delete-bytes! "file"
  [^URI uri opts]
  (let [file (io/file uri)]
    (io/delete-file file)))