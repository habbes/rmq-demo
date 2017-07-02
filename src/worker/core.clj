(ns worker.core
  (:gen-class)
  (:require [langohr.core :as rmq]
            [langohr.channel :as lch]
            [langohr.queue :as lq]
            [langohr.basic :as lb]
            [langohr.consumers :as lc]
            [clojure.string :as s]
            [clojure.data.json :as json]))

(def job-queue "jobs")

(defn simulate-computation
  "Simulates a long-running CPU-intensive computation.
  Reverses the input string after a forced delay"
  [input]
  (Thread/sleep 5)
  (s/reverse input))

(defn handle-job
  "Handles job"
  [{:keys [input] :as job} ch delivery-tag]
  (assoc job :result (simulate-computation input))
  (lb/ack ch delivery-tag))

(defn print-job-info
  "Prints info"
  [{:keys [input jobId serverId clientId]}]
  (println "Job received" "input:" input "jobId:" jobId "client:" clientId "server:" serverId))

(defn to-json
  "Converts raw message payload to json"
  [payload]
  (-> payload
      (String. "utf-8")
      (json/read-str :key-fn keyword)))

(defn on-message
  "Handles incoming message"
  [ch {:keys [delivery-tag]} payload]
  (let [data (to-json payload)]
    (print-job-info data)
    (handle-job data ch delivery-tag)))

(defn -main
  [& args]
  (with-open [conn (rmq/connect)]
    (let [ch (lch/open conn)]
      (lq/declare ch job-queue {:durable true :auto-delete false})
      (lb/qos ch 1)
      (println "Worker up. Waiting for jobs")
      (lc/blocking-subscribe ch job-queue on-message))))
