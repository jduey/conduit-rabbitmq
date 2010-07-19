(ns net.intensivesystems.conduit-rabbitmq
  (:use
     [net.intensivesystems.conduit :only [conduit conduit-run new-proc
                                          new-id seq-fn seq-proc a-run
                                          scatter-gather-fn
                                          reply-proc constant-stream]]
     net.intensivesystems.arrows)
  (:import
     [java.util UUID]))

(declare *channel*)
(declare *exchange*)

(defn declare-queue [queue]
    (.queueDeclare *channel* queue false false false {})
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

(defn msg-stream 
  ([queue]
   (let [consumer (consumer queue)]
     {:type :rabbitmq
      :fn (fn this-fn [x]
            (try
              [[(.nextDelivery consumer)] this-fn] 
              (catch InterruptedException e
                nil)))}))
  ([queue msecs]
   (let [consumer (consumer queue)]
     {:type :rabbitmq
      :fn (fn this-fn [x]
            (let [msg (.nextDelivery consumer msecs)]
              (when msg
                [[msg] this-fn])))})))

(defn rabbitmq-proc [source proc-fn]
    (let [id (new-id)
          source (str source)]
      {:type :rabbitmq
       :fn proc-fn
       :parts {source {:type :rabbitmq
                             id proc-fn}}
       :source source
       :id id}))

(with-arrow conduit
  (defn rabbitmq-handler [p queue]
    (->> (dissoc (get-in p [:parts queue]) :type)
         seq
         (mapcat (fn [[x f]] [x {:fn f}]))
         (apply a-select)))

  (defmethod conduit-run :rabbitmq [p queue channel exchange & [msecs]]
    (binding [*channel* channel
              *exchange* exchange]
      (let [queue (str queue)]
        (a-run
          (a-seq (if msecs
                   (msg-stream queue msecs)
                   (msg-stream queue))
                 (a-arr (fn [m]
                          [(read-msg m) m]))
                 (a-nth 0 (rabbitmq-handler p queue))
                 (a-nth 1 (a-arr ack-message))
                 (a-arr first))))))
            
  (defn rabbitmq-arr [source f]
    (rabbitmq-proc source (:fn (a-arr f)))))

(defmethod new-proc :rabbitmq [old-rabbitmq new-fn]
  (let [id (new-id)]
    (-> old-rabbitmq
      (assoc-in [:parts (:source old-rabbitmq) id] new-fn)
      (assoc :id id
             :fn new-fn))))

(defn publisher [p]
  (fn this-fn [x]
    (publish (:source p) [(:id p) x])
    [[] this-fn]))

(defmethod seq-proc :rabbitmq [p1 p2]
  (let [id (new-id)
        new-fn (seq-fn (:fn p1) (publisher p2))
        new-parts (merge-with merge
                              {(:source p1) {id new-fn}}
                              (:parts p1)
                              (:parts p2))]
    (assoc p1
           :id id
           :fn new-fn
           :parts new-parts)))

(defmethod scatter-gather-fn :rabbitmq [p]
  (fn this-fn [x]
    (let [reply-queue (str (UUID/randomUUID))]
      (.queueDeclare *channel* reply-queue false false false {})
      (.queueBind *channel* reply-queue *exchange* reply-queue)
      (publish (:source p) [(:id p) [x reply-queue]])
      (fn []
        (let [msg (get-msg reply-queue)]
          (ack-message msg)
          [(read-msg msg) this-fn])))))

(defmethod reply-proc :rabbitmq [p]
  (new-proc p
            (partial (fn this-fn [f [x reply-queue]]
                       (let [[new-x new-f] (f x)]
                         (publish reply-queue new-x)
                         [[] (partial this-fn new-f)]))
                     (:fn p))))

