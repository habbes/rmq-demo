(ns worker.util
  (:require [clojure.data.json :as json]))

(defn generate-id
  "Generates random id"
  []
  (apply str (take 16 (repeatedly #(char (+ (rand 26) 97))))))

(defn parse-json
  "Converts raw binary message payload to json"
  [payload]
  (-> payload
      (String. "utf-8")
      (json/read-str :key-fn keyword)))

(defn rand-between
  "Generates random int between a (inclusive)
  and b (exclusive)"
  [a b]
  (+ a (rand-int (- b a))))

(defn env
  "Gets the environment variable specified by key, or
  dft if the variable does not exist."
  [key dft]
  (or (System/getenv key) dft))
