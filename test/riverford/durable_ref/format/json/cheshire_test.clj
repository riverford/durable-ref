(ns riverford.durable-ref.format.json.cheshire-test
  (:require [clojure.test :refer :all]
            [riverford.durable-ref.format.json.cheshire]
            [riverford.durable-ref.core :as dref]))

(deftest test-json-serialization-round-trips
  (are [x]
    (= (dref/deserialize (dref/serialize x "json" {}) "json" {}) x)
    42
    "fred"
    "ethel"
    42.5
    nil
    {"foo" [1 2 "bar" nil]}
    [1 2 3]
    (range 99)))

(deftest test-fress-zip-serialization-round-trips
  (are [x]
    (= (dref/deserialize (dref/serialize x "json.zip" {}) "json.zip" {}) x)
    42
    "fred"
    "ethel"
    42.5
    nil
    {"foo" [1 2 "bar" nil]}
    [1 2 3]
    (range 99)))