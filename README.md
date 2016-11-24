# durable-ref


> Call him Voldemort, Harry. Always use the proper name for things. Fear of a name increases fear of the thing itself.”
>
> J.K. Rowling


Provides durable clojure reference types where the values are held off-heap/remotely. With an emphasis on the ability to _share_ them.

Such references are available across machines and programs and can persist across JVM restarts.

## Usage

Available via clojars:

```clojure
[riverford/durable-ref "0.1.0"]
```

Begin with the [tutorial](#Tutorial)

## Provided References

- [Value reference](#Value references)

  An immutable weakly interned, caching reference.

- [Volatile reference](#Volatile references)

  A stable mutable reference, uncoordinated.

## Provided Storages
- [Memory](#Memory)
- [File](#File)
- [Amazon S3](#Amazon S3)

## Provided Formats
- [EDN](#EDN)
- [Fressian](#Fressian)
- [Json](#Json)

## Rationale

It is often useful to be able to refer to a value across machines or to preserve
a reference to a value across restarts and so on. Particularily when the value is large and impractical to convey directly.

Often you will see this:

`(put-value! storage k v)`

And later:

`(get-value storage k)`

`k` is then a reference in your program to the value in storage.

This is sufficient for many programs. It is close to typical object-oriented style, and allows for different storage implementations.

However there are some problems with this using this style to reference values:
- `k` Does not reflect any properties of the reference, e.g can I rely on it being immutable?
- storage itself and storage format are often complected together.
- The referencing scheme needs to be known by code that wants to do look ups (e.g what is `k`?, do I need an equivalent `storage` instance?)

This library defines an extensible URI based set of conventions that allows different reference
mechanisms to be implemented on top of the same storages and formats.

Reference a value like this:

`value:s3://my-bucket/my-path/a5e744d0164540d33b1d7ea616c28f2fa97e754a.edn`

Or like this:

`value:file:///Users/me/obj/a5e744d0164540d33b1d7ea616c28f2fa97e754a.json`

Or a volatile (mutable) reference like this:

`volatile:file:///Users/me/people/fred.edn.zip`

This approach conveys several benefits:

- All information required to deref a value is encoded in the reference itself (e.g location, format, path).
- Storage and format are seperate components, independent of reference semantics and can be changed (and extended) independently.
- Semantics of the reference are encoded in the scheme, allowing one to e.g leverage the persistence/immutablity of the reference
to e.g cache values pervasively.
- URI's are shareable, and themselves values. Have an in-memory map reference many enormous data structures but retain the benefits of immutabity of the map only references values.

I hope this library starts the conversation on how durable reference types should be implemented in a consistent way. The goal being that the references can be shared between programs
without them having knowledge of one anothers internals.

## A note on performance

Because value references require immutability, programs are able to cache the value of the reference across all instances of that reference.

This is done internally by weak-interning results of `(reference uri)` calls where the uri denotes a value (as opposed to a mutable or ambiguous reference).

This means you can maintain an arbitrary number of aliases of the reference and only pay the cost of deref once (or until all instances of the reference are GC'd)

There is no caching of deref results on mutable references.

## Tutorial

The api for references is in the `riverford.durable-ref.core` namespace.

```clojure
(require '[riverford.durable-ref.core :as dref])
```

Pick a suitable directory on your machine for storing values. I am going to use `/Users/danielstone/objects`

### Value references

Obtain a durable reference to a value with `persist`. Passing a base-uri (directory)
object and optional opts (e.g `{:format "json"}`).

```clojure
(def fred-ref
 (persist "file:///Users/me/objects"
  {:name "fred"
   :age 42}))
fred-ref
;; =>
#object[riverford.durable_ref.core.DurableValueRef
        "value:file:///users/danielstone/objects/7664124773263ad3bda79e9267e1793915c09e2d.edn"]
```

Notice the reference URI you get back includes a sha1 identity hash of the object.

references implement `clojure.lang.IDeref`

```clojure
@fred-ref
;; =>
{:name "fred", :age 42}
```

references can be derefed explicitly, perhaps to signal the fact a deref could fail (due to unavailability of storage)

```clojure
(dref/deref fred-ref)
;; =>
{:name "fred" :age 42}
```

`deref` also supports additional options (e.g to forward to storage and format implementations).

You can obtain a URI to the reference
```clojure
(dref/uri fred-ref)
;; =>
#object[java.net.URI 0x437f6d9e "value:file:///users/danielstone/objects/7664124773263ad3bda79e9267e1793915c09e2d.edn"]
```

Most ref operations such as `deref` support using a URI or string directly.
```clojure
(dref/deref "value:file:///users/danielstone/objects/7664124773263ad3bda79e9267e1793915c09e2d.edn")
;; =>
{:name "fred", :age 42}
```

Values are cached for value references, and reference instances themselves are weak-interned via
a WeakHashMap. Repeated `deref`/`persist!` calls on the same value will be very cheap while reference instances are on the heap.

If storage changes, value references will throw on deref.

`reference` reacquires a reference object of the correct type from a URI or string.

```clojure
(dref/reference "value:file:///users/danielstone/objects/7664124773263ad3bda79e9267e1793915c09e2d.edn")
;; =>
#object[riverford.durable_ref.core.DurableValueRef
        "value:file:///users/danielstone/objects/7664124773263ad3bda79e9267e1793915c09e2d.edn"]
```

#### Consistency

Even if your storage does not immediately reflect your write, its ok as long as you retain the reference
returned by `persist`, this is because the value is pre-cached. Due to reference weak-interning, you can alias it
as a URI or string and as long as the reference hasn't been GC'd, you will continue to see the value.

### Volatile references

Rather than using persist!, mutable references are first named explicitly. So decide on an absolute URI.

e.g

`volatile:file:///users/danielstone/objects/fred.edn`

You can `deref` it like normal (even if its never been written to).

```clojure
(dref/deref "volatile:file:///users/danielstone/objects/fred.edn")
;; =>
nil
```

You can mutate the ref with `overwrite!`
```clojure
(dref/overwrite! "volatile:file:///users/danielstone/objects/fred.edn" {:name "fred"})
;; =>
nil

;; be aware, that the ability to read immediately
;; is determined by the consistency properties of your storage
;; (always assume possibilty of stale values)
(dref/deref "volatile:file:///users/danielstone/objects/fred.edn")
;; =>
{:name "fred"}
```

You can call `reference` on it to acquire reference object.
```clojure
(def fred-mut-ref (dref/reference "volatile:file:///users/danielstone/objects/fred.edn"))
fred-mut-ref
;; =>
#object[riverford.durable_ref.core.DurableVolatileRef "volatile:file:///users/danielstone/objects/fred.edn"]
```

The reference object implements `clojure.lang.IDeref`
```clojure
@fred-mut-ref
;; =>
{:name "fred"}
```

Finally mutable refs can be deleted (when the storage supports it) with `delete!`
```clojure
(dref/delete! fred-mut-ref)
;; =>
nil

@fred-mut-ref
;; =>
nil
```

## 'Batteries included' storages

### Memory

Scheme: `mem`

in-memory storage based on a global ConcurrentHashMap. Useful for testing.

### File

Scheme: `file`

Local disk backed durable storage.

### Amazon S3

Scheme: `s3`

#### using [amazonica](https://github.com/mcohen01/amazonica)

```clojure
:dependencies [amazonica "0.3.77"]
(require '[riverford.durable-ref.scheme.s3.amazonica])

;; Storage options (optionally provide in an options map to persist, deref, overwrite!, delete!)
;; see amazonica documentation for more information
{:scheme {:s3 {:amazonica {:shared-opts {} ;; spliced into all amazonica requests
                          :read-opts {}  ;; spliced into get-object requests
                          :write-opts {} ;; spliced into put-object requests
                          :delete-opts {} ;; spliced into delete-object requests
                          }}}}

```

## 'Batteries included' formats

### EDN

Extensions (`edn`, `edn.zip`)

Serialization using `clojure.edn` and `pr`

### Fressian

Extensions (`fressian`, `fressian.zip`)

Serialization via [data.fressian](https://github.com/clojure/data.fressian)

```clojure
:dependencies [org.clojure/data.fressian "0.2.1"]
(require '[riverford.durable-ref.format.fressian])

;; Format options (optionally provide in an options map to persist, deref, overwrite!, delete!)
;; see fressian docs for more details
{:format {:fressian {:read-opts {} ;; spliced into create-reader calls
                     :write-opts {} ;; spliced into create-writer calls
                     }}}
```

### Json

Extensions (`json`, `json.zip`)

#### Using [cheshire](https://github.com/dakrone/cheshire)

```clojure
:dependencies [cheshire "5.6.3"]
(require '[riverford.durable-ref.format.json.cheshire])

;; Format options (optionally provide in an options map to persist, deref, overwrite!, delete!)
;; see cheshire docs for more details
{:format {:json {:cheshire  {:write-opts {} ;; passed as options to generate-stream calls
                             :read-opts {
                               :key-fn f1 ;; passed as the `key-fn` arg to parse-stream calls
                               :array-coerce-fn f2 ;; passed as the `array-coerce-fn` to parse-stream calls
                              }}}}}
```

## Extension

### Storage

There are 3 multimethods you can implement currently (dispatching on the scheme):
- `read-bytes`, receives the uri and options passed to `deref`. Returns a byte array or nil.
- `write-bytes!`, receives the uri, the serialized byte array and options passed to `persist!`,`overwrite!.`
- (optional) `delete-bytes!`, receives the uri and options passed to `delete!`.

### Formats

There are 2 multimethods to implement dispatching on the format string:
- `serialize`, receives the object, the format string, and options passed to `persist!`, `overwrite!`
- `deserialize`, receives the serialized byte array, the format string and options passed to `deref`.

## TODO

- CAS/Atomic references
- more formats and storages

Pull requests welcome!

## License

Copyright © 2016 Riverford Organic Farmers Ltd

Distributed under the [3-clause license ("New BSD License" or "Modified BSD License").](http://github.com/riverford/durable-ref/blob/master/LICENSE)