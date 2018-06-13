(ns ^:integration riverford.durable-ref.scheme.redis.carmine-integration-test
  (:require [clojure.test :refer :all]
            [riverford.durable-ref.scheme.redis.carmine]
            [riverford.durable-ref.core :as dref])
  (:import (java.net URI)
           (java.util UUID)))

(def uri-base
  (or (System/getenv "DURABLE_REF_TEST_REDIS_PREFIX")
      (System/getProperty "durable.ref.test.redis.prefix")
      (.println System/err "Warning: DURABLE_REF_TEST_REDIS_PREFIX not set, skipping some tests.")))

(deftest test-retrieve-non-existant-returns-nil
  (when uri-base
    (let [uri (URI. (str uri-base "/0/test-" (UUID/randomUUID)))]
      (is (nil? (dref/read-bytes uri {}))))))

(deftest test-store-retrieve-round-trip
  (when uri-base
    (let [bytes (byte-array (shuffle (vec (.getBytes "loremipsumfoobardsfsdfsdfsfdsfdsf"))))
          uri (URI. (str uri-base "/0/test-" (UUID/randomUUID)))]
      (dref/write-bytes! uri bytes {})
      (is (= (seq bytes)
             (seq (dref/read-bytes uri {}))))
      (dref/delete-bytes! uri {}))))

(deftest test-store-delete-cycle
  (when uri-base
    (let [bytes (byte-array (shuffle (vec (.getBytes "loremipsumfoobardsfsdfsdfsfdsfdsf"))))
          uri (URI. (str uri-base "/0/test-" (UUID/randomUUID)))]
      (dref/write-bytes! uri bytes {})
      (dref/delete-bytes! uri {})
      (is (nil? (dref/read-bytes uri {}))))))

(deftest test-concurrent-swaps
  (when uri-base
    (let [uri (URI. (str "atomic:" uri-base "/0/test-" (UUID/randomUUID) ".edn"))
          sub-uri (URI. (.getSchemeSpecificPart uri))
          futs (doall (repeatedly 25 #(future (Thread/sleep (rand-int 1000))
                                               (dref/do-atomic-swap! sub-uri (fnil inc 0) {}))))]
      (run! deref futs)
      (is (= 25 (dref/value uri)))
      (dref/delete-bytes! sub-uri {}))))
