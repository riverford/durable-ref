(ns riverford.durable-ref.format.fressian-test
  (:require [clojure.test :refer :all]
            [riverford.durable-ref.format.nippy]
            [riverford.durable-ref.core :as dref]))

(deftest test-nippy-serialization-round-trips
  (are [x]
    (= (dref/deserialize (dref/serialize x "nippy" {}) "nippy" {}) x)
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