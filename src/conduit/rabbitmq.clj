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
  ([queue] (.nextDelivery (consumer queue)))
  ([queue msecs] (.nextDelivery (consumer queue) msecs)))

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
                      x)
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
               ))))


  #_(let [source (str source)
        id (str id)
        reply-id (str id "-reply")]
    {:type :rabbitmq
     :created-by (:created-by proc)
     :args (:args proc)
     :source source
     :id id
     :reply (rabbitmq-pub-reply source reply-id)
     :no-reply (rabbitmq-pub-no-reply source id)
     :scatter-gather (rabbitmq-sg-fn source reply-id)
     :parts (merge-with merge (:parts proc)
                        {source {id (:no-reply proc)
                                 reply-id (reply-fn (:reply proc))}})}))


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
      (let [msg (wait-for-message consumer msecs)
            [conduit-msg c-queue] (read-msg msg)
            [new-selector new-c] (binding [*conduit-rabbitmq-msg* conduit-msg]
                                   (selector conduit-msg))
            reply-queue (read-msg (get-msg c-queue msecs))]
        (if (nil? reply-queue)
          (new-c nil)
          (publish reply-queue (new-c identity)))
        (ack-message msg)
        (message-handler new-selector msecs))
      (catch Throwable _
        curr-fn))))

(defn rabbitmq-run [p queue channel exchange & [msecs]]
  (when-let [handler-map (get-in p [:parts queue])]
    (binding [*channel* channel
              *exchange* exchange]
      (let [queue (str queue)
            selector (select-fn handler-map)
            consumer (consumer queue)]
        (declare-queue queue)
        (loop [handle-msg (message-handler selector msecs)]
          (recur (handle-msg consumer)))))))
