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
