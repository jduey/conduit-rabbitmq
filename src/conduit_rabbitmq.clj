(ns conduit-rabbitmq
  (:use
     conduit)
  (:import
   [com.rabbitmq.client Connection ConnectionFactory Channel
    MessageProperties QueueingConsumer]
   [java.util UUID]))

(declare *channel*)
(declare *exchange*)

(defn declare-queue [queue]
  (.queueDeclare *channel* queue false false false false {})
  (.queueBind *channel* queue *exchange* queue))

(defn purge-queue [queue]
  (.queuePurge *channel* queue))

(defn publish [queue msg]
  (let [msg-str (binding [*print-dup* true]
                  (pr-str msg))]
    (.basicPublish *channel* *exchange* queue
                   (MessageProperties/PERSISTENT_TEXT_PLAIN)
                   (.getBytes msg-str))))

(defn ack-message [msg]
  (.basicAck *channel*
             (.getDeliveryTag (.getEnvelope msg))
             false))

(defn consumer [queue]
  (let [consumer (QueueingConsumer. *channel*)]
    (.basicConsume *channel* queue, false, consumer)
    consumer))

(defn read-msg [m]
  (read-string (String. (.getBody m))))

(defn get-msg
  ([queue] (try
            (.nextDelivery (consumer queue))
            (catch InterruptedException e
              nil)))
  ([queue msecs] (.nextDelivery (consumer queue) msecs)))

(defn msg-stream [queue & [msecs]]
  (let [consumer (consumer queue)]
    (if msecs
      (fn this-fn1 [x]
        (let [msg (.nextDelivery consumer msecs)]
          (when msg
            [[msg] this-fn1])))
      (fn this-fn2 [x]
        (try
          (let [msg (.nextDelivery consumer)]
            [[msg] this-fn2])
          (catch InterruptedException e
            nil))))))

(defn msg-stream-proc [queue & [msecs]]
  {:type :rabbitmq
   :fn (msg-stream queue msecs)})

(defn a-rabbitmq [source id proc]
  (let [source (str source)]
    (assoc proc
           :type :rabbitmq
           :parts {source {:type :rabbitmq
                           id proc}}
           :source source
           :id id)))

(defn rabbitmq-run [p queue channel exchange & [msecs]]
  (binding [*channel* channel
            *exchange* exchange]
    (let [queue (str queue)
          get-next (msg-stream queue msecs)]
      (when-let [handler-map (get-in p [:parts queue])]
        (loop [[[raw-msg] get-next] (get-next nil)]
          (when raw-msg
            (let [[target msg] (read-msg raw-msg)]
              (try
                (when-let [handler (get handler-map target)]
                  ((:fn handler) msg))
                (ack-message raw-msg))
              (recur (get-next nil)))))))))

(defn publisher [p]
  (fn this-fn [x]
    (publish (:source p) [(:id p) x])
    [[] this-fn]))

(defmethod seq-proc :rabbitmq [p1 p2]
  (let [new-fn (seq-fn (:fn p1) (publisher p2))
        new-parts (merge-with merge
                              (:parts p1)
                              (:parts p2))]
    (assoc p1
      :fn new-fn
      :parts new-parts)))

(defmethod scatter-gather-fn :rabbitmq [p]
  (fn this-fn [x]
    (let [reply-queue (str (UUID/randomUUID))]
      (.queueDeclare *channel* reply-queue false false false true {})
      (.queueBind *channel* reply-queue *exchange* reply-queue)
      (publish (:source p) [(:id p) [x reply-queue]])
      (fn []
        (let [msg (get-msg reply-queue)]
          (ack-message msg)
          [(read-msg msg) this-fn])))))


(defmethod reply-proc :rabbitmq [p]
  (let [id (pr-str (:id p) "_reply")
        new-fn (partial (fn this-fn [f [x reply-queue]]
                       (let [[new-x new-f] (f x)]
                         (publish reply-queue new-x)
                         [[] (partial this-fn new-f)]))
                     (:fn p))]
    (-> p
        (assoc-in [:parts (:source p) id] {:fn new-fn})
        (assoc :id id
               :fn new-fn))))
