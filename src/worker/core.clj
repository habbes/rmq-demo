(ns worker.core
  (:gen-class)
  (:require [langohr.core :as rmq]
            [langohr.channel :as lch]
            [langohr.exchange :as le]
            [langohr.queue :as lq]
            [langohr.basic :as lb]
            [langohr.consumers :as lc]
            [clojure.string :as s]
            [clojure.data.json :as json]))

(def jobs-q "jobs")
(def events-x "events")

(defn generate-id
  "Generates random id"
  []
  (apply str (take 16 (repeatedly #(char (+ (rand 26) 65))))))

(def worker-id (generate-id))

(defn to-json
  "Converts raw message payload to json"
  [payload]
  (-> payload
      (String. "utf-8")
      (json/read-str :key-fn keyword)))

(defn rand-between
  "Generates random int between a (inclusive)
  and b (exclusive)"
  [a b]
  (+ a (rand (- b a))))

(defn print-job-info
  "Prints info"
  [{:keys [input jobId serverId clientId]}]
  (println "Job received" "input:" input "jobId:" jobId
           "client:" clientId "server:" serverId))

(defn simulate-computation
  "Simulates a long-running CPU-intensive computation.
  Reverses the input string after a forced delay"
  [input]
  (Thread/sleep (* 1000 (rand-between 5 11)))
  (s/reverse input))

(defn emit-event
  "Publishes a message to the events exchange"
  [ch msg]
  (->> msg
       json/write-str
       (lb/publish ch events-x "")))

(defn handle-job
  "Handles job"
  [{:keys [input] :as job} ch delivery-tag]
  (let [result (conj job {:type "result"
             :workerId worker-id
             :result (simulate-computation input)})]
    (lb/ack ch delivery-tag)
    (emit-event ch result)))

(defn on-message
  "Handles incoming message"
  [ch {:keys [delivery-tag]} payload]
  (let [data (to-json payload)]
    (print-job-info data)
    (future (handle-job data ch delivery-tag))))

(defn -main
  [& args]
  (with-open [conn (rmq/connect)]
    (let [ch (lch/open conn)]
      (lq/declare ch jobs-q {:durable true :auto-delete false})
      (le/fanout ch events-x {:durable false :auto-delete false})
      (lb/qos ch 1)
      (println "Worker" worker-id "Waiting for jobs")
      (lc/blocking-subscribe ch jobs-q on-message))))
