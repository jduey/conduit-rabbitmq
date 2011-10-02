(ns conduit.rabbitmq
  (:use conduit.core)
  (:import [com.rabbitmq.client Connection ConnectionFactory Channel
            MessageProperties QueueingConsumer]
           [java.util UUID]))

(declare ^:dynamic *channel*)
(declare ^:dynamic *exchange*)
(def ^:dynamic *conduit-rabbitmq-msg* nil)

(defmacro with-rabbitmq [[channel exchange] & body]
  `(binding [*channel* ~channel
             *exchange* ~exchange]
     ~@body))

(defn declare-queue
  ([queue]
     (declare-queue queue false))
  ([queue autodelete]
     (.queueDeclare *channel* queue true false autodelete {})
     (.queueBind *channel* queue *exchange* queue)))

(defn purge-queue [queue]
  (.queuePurge *channel* queue))

(defn consumer [queue]
  (let [consumer (QueueingConsumer. *channel*)]
    (.basicConsume *channel* queue false consumer)
    consumer))

(defn publish [queue msg]
  (let [msg-str (binding [*print-dup* true]
                  (pr-str msg))]
    (.basicPublish *channel* *exchange* queue
                   (MessageProperties/PERSISTENT_TEXT_PLAIN)
                   (.getBytes msg-str))))

(defn get-msg
  ([queue]
   (try
     (.nextDelivery (consumer queue))
     (catch InterruptedException _
       nil)))
  ([queue msecs]
   (.nextDelivery (consumer queue) msecs)))

(defn read-msg [m]
  (read-string (String. (.getBody m))))

(defn ack-message [msg]
  (.basicAck *channel*
             (.getDeliveryTag (.getEnvelope msg))
             false))

(defn a-rabbitmq
  ([source id proc] (a-rabbitmq source nil id proc))
  ([source possible-queues id proc]
   (with-meta
     (fn curr-fn [x]
       (let [source (if (fn? source)
                      (source x)
                      source)
             c-queue (str (UUID/randomUUID))]
         (declare-queue c-queue true)
         (publish source [[id x] c-queue])
         [curr-fn
          (fn [c]
            (if (nil? c)
              (publish c-queue nil)
              (let [reply-queue (str (UUID/randomUUID))
                    _ (declare-queue reply-queue true)
                    _ (publish c-queue reply-queue)
                    reply (get-msg reply-queue)]
                (ack-message reply)
                (c (read-msg reply)))))]))
     (-> (meta proc)
        (select-keys [:created-by :args :type])
        (assoc :transport :rabbitmq
               :parts {source {id proc}})))))


(defn wait-for-message [consumer msecs]
  (try
   (if msecs
      (.nextDelivery consumer msecs)
      (.nextDelivery consumer))
    (catch InterruptedException e
      nil)))

(defn message-handler [selector msecs]
  (fn curr-fn [consumer]
    (try
      (when-let [msg (wait-for-message consumer msecs)]
        (let [[conduit-msg c-queue] (read-msg msg)
              [new-selector new-c] (binding [*conduit-rabbitmq-msg* conduit-msg]
                                     (selector conduit-msg))
              reply-queue (read-msg (get-msg c-queue msecs))]
          (if (nil? reply-queue)
            (new-c nil)
            (publish reply-queue (new-c identity)))
          (ack-message msg)
          (message-handler new-selector msecs)))
      (catch InterruptedException _
        nil)
      (catch Throwable e
        curr-fn))))

(defn rabbitmq-run [p queue channel exchange & [msecs]]
  (when-let [handler-map (get-in (meta p) [:parts queue])]
    (binding [*channel* channel
              *exchange* exchange]
      (let [queue (str queue)
            msecs (or msecs 100)
            _ (declare-queue queue)
            selector (select-fn handler-map)
            consumer (consumer queue)]
        (loop [handle-msg (message-handler selector msecs)]
          (when handle-msg
            (recur (handle-msg consumer))))))))
