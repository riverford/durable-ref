(ns ^:integration riverford.durable-ref.scheme.s3.amazonica-integration-test
  (:require [clojure.test :refer :all]
            [riverford.durable-ref.scheme.s3.amazonica]
            [riverford.durable-ref.core :as dref])
  (:import (java.net URI)
           (java.util UUID)))

(def uri-base
  (or (System/getenv "DURABLE_REF_TEST_S3_PREFIX")
      (System/getProperty "durable.ref.test.s3.prefix")
      (.println System/err "Warning: DURABLE_REF_TEST_S3_PREFIX not set, skipping some tests.")))

(deftest test-retrieve-non-existant-returns-nil
  (when uri-base
    (let [uri (URI. (str uri-base "/test-" (UUID/randomUUID)))]
      (is (nil? (dref/read-bytes uri {}))))))

(deftest test-store-retrieve-round-trip
  (when uri-base
    (let [bytes (byte-array (shuffle (vec (.getBytes "loremipsumfoobardsfsdfsdfsfdsfdsf"))))
          uri (URI. (str uri-base "/test-" (UUID/randomUUID)))]
      (dref/write-bytes! uri bytes {})
      (is (= (seq bytes)
             (seq (dref/read-bytes uri {}))))
      (dref/delete-bytes! uri {}))))

(deftest test-store-delete-cycle
  (when uri-base
    (let [bytes (byte-array (shuffle (vec (.getBytes "loremipsumfoobardsfsdfsdfsfdsfdsf"))))
          uri (URI. (str uri-base "/test-" (UUID/randomUUID)))]
      (dref/write-bytes! uri bytes {})
      (dref/delete-bytes! uri {})
      (is (nil? (dref/read-bytes uri {}))))))