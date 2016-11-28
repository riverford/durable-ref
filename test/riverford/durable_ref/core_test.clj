(ns riverford.durable-ref.core-test
  (:require [clojure.test :refer :all]
            [riverford.durable-ref.core :as dref]
            [clojure.string :as str]
            [clojure.java.io :as io])
  (:import (java.util UUID)
           (java.net URI)))

(deftest test-persist-immediate-deref-equiv
  (is (= @(dref/persist "mem://tests" 42)
         42))
  (is (= (dref/value (dref/persist "mem://tests" {:name "fred"}))
         {:name "fred"})))

(deftest test-can-return-uri-of-ref
  (is (= (str (dref/uri (dref/reference "mem://tests/foo.edn")))
         "mem://tests/foo.edn"))
  (is (= (str (dref/uri (dref/reference "volatile:mem://tests/foo.edn")))
         "volatile:mem://tests/foo.edn")))

(deftest test-persist-delayed-deref-equiv
  (let [uuid (UUID/randomUUID)
        ref (dref/persist "mem://tests" uuid)]
    (is (= (dref/value (dref/uri ref))
           @ref
           uuid))))

(deftest test-overwrite-deref-cycle
  (let [ref (format "volatile:mem://tests/%s.edn" (UUID/randomUUID))]
    (is (nil? (dref/value ref)))
    (dref/overwrite! ref :foo)
    (is (= (dref/value ref)
           (dref/value (URI. ref))
           @(dref/reference ref)
           @(dref/reference (URI. ref))
           :foo))
    (dref/overwrite! ref :bar)
    (is (= (dref/value ref)
           (dref/value (URI. ref))
           @(dref/reference ref)
           @(dref/reference (URI. ref))
           :bar))))

(deftest test-delete-cycle
  (let [ref (format "volatile:mem://tests/%s.edn" (UUID/randomUUID))]
    (is (nil? (dref/value ref)))
    (dref/overwrite! ref :foo)
    (is (= (dref/value ref)
           (dref/value (URI. ref))
           @(dref/reference ref)
           @(dref/reference (URI. ref))
           :foo))
    (dref/delete! ref)
    (is (nil? (dref/value ref)))
    (dref/delete! ref)
    (is (nil? (dref/value ref)))))

(deftest test-persist-idempotency
  (let [uuid  (UUID/randomUUID)
        ref (dref/persist "mem://tests" uuid)]
    (is (= ref
           (dref/persist "mem://tests" uuid)
           (dref/persist "mem://tests" uuid)
           (dref/persist "mem://tests" uuid)
           (dref/reference (dref/uri ref))))))

(deftest test-persist-interning
  (let [uuid  (UUID/randomUUID)
        ref (dref/persist "mem://tests" uuid)]
    (is (identical? ref (dref/persist "mem://tests" uuid)))
    (is (identical? ref (dref/reference (dref/uri ref))))))

(deftest test-value-ref-tutorial-correct
  (let [dir (System/getProperty "java.io.tmpdir")
        base-uri (str/lower-case (str "file://" dir))
        fred {:name "fred", :age 42}
        fred-ref (dref/persist base-uri fred)]
    (is (instance? riverford.durable_ref.core.DurableValueRef fred-ref))
    (is (satisfies? dref/IDurableRef fred-ref))
    (is (= @fred-ref fred))
    (is (= (dref/value fred-ref) fred))
    (is (= (dref/uri fred-ref)
           (URI. (str "value:" base-uri "7664124773263ad3bda79e9267e1793915c09e2d.edn"))))
    (is (= (dref/value (str "value:" base-uri "7664124773263ad3bda79e9267e1793915c09e2d.edn"))
           fred))))

(deftest test-volatile-ref-tutorial-correct
  (let [dir (System/getProperty "java.io.tmpdir")
        uri (str "volatile:file://" (str/lower-case dir) "fred.edn")
        fred {:name "fred"}]
    (io/delete-file (.getSchemeSpecificPart (URI. uri)) true)
    (is (nil? (dref/value uri)))
    (is (nil? (dref/overwrite! uri fred)))
    (is (= fred (dref/value uri)))
    (let [fred-ref (dref/reference uri)]
      (is (instance? riverford.durable_ref.core.DurableVolatileRef fred-ref))
      (is (satisfies? dref/IDurableRef fred-ref))
      (is (= fred @fred-ref))
      (dref/delete! fred-ref)
      (is (nil? @fred-ref)))))

(deftest test-atomic-ref-tutorial-correct
  (let [uri "atomic:mem://tmp/fred.edn"
        fred {:name "fred"}]
    (dref/delete! uri)
    (is (nil? (dref/value uri)))
    (is (= 1 (dref/atomic-swap! uri (fnil inc 0))))
    (is (= 2 (dref/atomic-swap! uri (fnil inc 0))))
    (is (nil? (dref/overwrite! uri fred)))
    (let [fred-ref (dref/reference uri)]
      (is (instance? riverford.durable_ref.core.DurableAtomicRef fred-ref))
      (is (satisfies? dref/IDurableRef fred-ref))
      (is (= fred @fred-ref))
      (is (= {:name "fred"
              :age 42}) (swap! fred-ref assoc :age 42))
      (dref/delete! fred-ref)
      (is (nil? @fred-ref)))))

(deftest test-value-ref-mutation-attempt-throws
  (let [ref (dref/persist "mem://tests" (UUID/randomUUID))]
    (is (thrown? Throwable (dref/overwrite! ref (UUID/randomUUID))))
    (is (thrown? Throwable (dref/delete! ref)))))

(deftest test-value-ref-mutated-in-storage-deref-throws
  (let [ref (dref/persist "mem://tests" (UUID/randomUUID))
        sneaky-ref (dref/reference (str "volatile:" (.getSchemeSpecificPart (dref/uri ref))))]
    (dref/overwrite! sneaky-ref (UUID/randomUUID))
    (dref/evict! ref)
    (is (thrown? Throwable @ref))))

(deftest test-ref-eq
  (are [x y]
    (= x y)
    (dref/reference "mem://tests/foo") (dref/reference "mem://tests/foo")
    (dref/reference "mem://tests/foo") (dref/->DurableReadonlyRef (URI. "mem://tests/foo"))
    (dref/persist "mem://tests/foo" 42) (dref/persist "mem://tests/foo" 42)
    (dref/reference "volatile:mem://tests/foo/bar") (dref/reference "volatile:mem://tests/foo/bar")
    (dref/reference "volatile:mem://tests/foo/bar") (dref/->DurableVolatileRef (URI. "volatile:mem://tests/foo/bar"))
    (dref/reference "atomic:mem://tests/foo/bar") (dref/reference "atomic:mem://tests/foo/bar")
    (dref/reference "atomic:mem://tests/foo/bar") (dref/->DurableAtomicRef (URI. "atomic:mem://tests/foo/bar"))))

(deftest test-ref-neq
  (are [x y]
    (not= x y)
    (dref/reference "mem://tests/foo") (dref/reference "mem://tests/foo2")
    (dref/reference "mem://tests/foo") (dref/->DurableVolatileRef (URI. "mem://tests/foo"))
    (dref/persist "mem://tests/foo" 42) (dref/persist "mem://tests/foo" :fred)
    (dref/reference "volatile:mem://tests/foo/bar") (dref/reference "mem://tests/foo/bar")
    (dref/reference "volatile:mem://tests/foo/bar") (dref/->DurableReadonlyRef (URI. "mem://tests/foo/bar"))
    (dref/reference "atomic:mem://tests/foo/bar") (dref/reference "mem://tests/foo/bar")
    (dref/reference "atomic:mem://tests/foo/bar") (dref/reference "volatile:mem://tests/foo/bar")
    (dref/reference "atomic:mem://tests/foo/bar") (dref/->DurableReadonlyRef (URI. "mem://tests/foo/bar"))
    (dref/reference "atomic:mem://tests/foo/bar") (dref/->DurableVolatileRef (URI. "mem://tests/foo/bar"))))

(deftest test-ref-hash-eq
  (are [x y]
    (= (hash x) (hash y))
    (dref/reference "mem://tests/foo") (dref/reference "mem://tests/foo")
    (dref/reference "mem://tests/foo") (dref/->DurableReadonlyRef (URI. "mem://tests/foo"))
    (dref/persist "mem://tests/foo" 42) (dref/persist "mem://tests/foo" 42)
    (dref/reference "volatile:mem://tests/foo/bar") (dref/reference "volatile:mem://tests/foo/bar")
    (dref/reference "volatile:mem://tests/foo/bar") (dref/->DurableVolatileRef (URI. "volatile:mem://tests/foo/bar"))
    (dref/reference "atomic:mem://tests/foo/bar") (dref/reference "atomic:mem://tests/foo/bar")
    (dref/reference "atomic:mem://tests/foo/bar") (dref/->DurableAtomicRef (URI. "atomic:mem://tests/foo/bar"))))

(deftest test-ref-hash-neq
  (are [x y]
    (not= (hash x) (hash y))
    (dref/reference "mem://tests/foo") (dref/reference "mem://tests/foo2")
    (dref/persist "mem://tests/foo" 42) (dref/persist "mem://tests/foo" :fred)
    (dref/reference "volatile:mem://tests/foo/bar") (dref/reference "mem://tests/foo/bar")
    (dref/reference "atomic:mem://tests/foo/bar") (dref/reference "mem://tests/foo/bar")
    (dref/reference "atomic:mem://tests/foo/bar") (dref/reference "volatile:mem://tests/foo/bar")))

(deftest test-edn-serialization-round-trips
  (are [x]
    (= (dref/deserialize (dref/serialize x "edn" {}) "edn" {}) x)
    42
    :fred
    "ethel"
    42M
    42N
    42.5
    2/3
    {:foo :bar}
    [1 2 3]
    (range 99)
    #{1, 2, 3}))

(deftest test-edn-zip-serialization-round-trips
  (are [x]
    (= (dref/deserialize (dref/serialize x "edn.zip" {}) "edn.zip" {}) x)
    42
    :fred
    "ethel"
    42M
    42N
    42.5
    2/3
    {:foo :bar}
    [1 2 3]
    (range 99)
    #{1, 2, 3}))

(deftest test-concurrent-swaps
  (let [uri (URI. (str "atomic:mem://test/" (UUID/randomUUID) ".edn"))
        futs (doall (repeatedly 10 #(future
                                      (dotimes [x 100]
                                        (Thread/sleep (rand-int 10))
                                        (dref/atomic-swap! uri (fnil inc 0) {})))))]
    (run! deref futs)
    (is (= 1000 (dref/value uri)))
    (dref/delete! uri)))

(deftest test-atom-interface
  (let [ref (dref/reference (URI. (str "atomic:mem://test/" (UUID/randomUUID) ".edn")))]
    (is (nil? @ref))
    (is (= 42 (reset! ref 42)))
    (is (= 42 (swap! ref identity)))
    (is (= 45 (swap! ref + 3)))
    (is (= 65 (swap! ref + 10 10)))
    (is (= 95 (swap! ref + 10 10 10)))
    (is (= 135 (swap! ref + 10 10 10 10)))))

(deftest test-atomic-interface
  (let [ref (URI. (str "atomic:mem://test/" (UUID/randomUUID) ".edn"))]
    (is (nil? (dref/value ref)))
    (is (nil? (dref/overwrite! ref 42)))
    (is (= 42 (dref/atomic-swap! ref identity)))
    (is (= 45 (dref/atomic-swap! ref #(+ % 3))))))