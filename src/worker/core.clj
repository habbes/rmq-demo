(ns worker.core
  (:gen-class)
  (:require [langohr.core :as rmq]
            [langohr.channel :as lch]
            [langohr.exchange :as le]
            [langohr.queue :as lq]
            [langohr.basic :as lb]
            [langohr.consumers :as lc]
            [clojure.string :as s]
            [clojure.data.json :as json]
            [worker.util :as util]))

(def jobs-q "jobs")
(def events-x "events")

(def worker-id (util/generate-id))

(defn print-job-info
  "Prints info"
  [{:keys [input jobId serverId clientId]}]
  (println "Job received" "input:" input "jobId:" jobId
           "client:" clientId "server:" serverId))

(defn simulate-computation
  "Simulates a long-running CPU-intensive computation.
  Reverses the input string after a forced delay"
  [input]
  (Thread/sleep (* 1000 (util/rand-between 5 11)))
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
  (let [data (util/parse-json payload)]
    (print-job-info data)
    (future (handle-job data ch delivery-tag))))

(defn -main
  [& args]
  (with-open [conn (rmq/connect {:uri (util/env "AMQP_URL" "amqp://localhost:5672")})]
    (let [ch (lch/open conn)]
      (lq/declare ch jobs-q {:durable true :auto-delete false})
      (le/fanout ch events-x {:durable false :auto-delete false})
      (lb/qos ch (Integer/parseInt (util/env "WORKER_MAX_JOBS" "1")))
      (println "Worker" worker-id "Waiting for jobs")
      (lc/blocking-subscribe ch jobs-q on-message))))
