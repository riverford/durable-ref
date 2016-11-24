(ns riverford.durable-ref.format.fressian-test
  (:require [clojure.test :refer :all]
            [riverford.durable-ref.format.fressian]
            [riverford.durable-ref.core :as dref]))

(deftest test-fress-serialization-round-trips
  (are [x]
    (= (dref/deserialize (dref/serialize x "fressian" {}) "fressian" {}) x)
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

(deftest test-fress-zip-serialization-round-trips
  (are [x]
    (= (dref/deserialize (dref/serialize x "fressian.zip" {}) "fressian.zip" {}) x)
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