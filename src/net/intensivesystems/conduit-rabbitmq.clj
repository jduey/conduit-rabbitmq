(ns net.intensivesystems.conduit-rabbitmq
  (:use
     clojure.test
     [clojure.pprint :only [pprint]]
     [net.intensivesystems.conduit :only [conduit conduit-run new-proc
                                          new-id seq-fn seq-proc a-run]]
     net.intensivesystems.arrows)
  (:import
     [com.rabbitmq.client Connection ConnectionFactory Channel
      MessageProperties QueueingConsumer]))

(defn rabbitmq-connection [host vhost user password]
  "Create a simple rabbitmq connection."
  (.newConnection
    (doto (ConnectionFactory.)
      (.setHost host)
      (.setVirtualHost vhost)
      (.setUsername user)
      (.setPassword password))))

(defn rabbitmq-channel [connection exchange queue]
  (doto (.createChannel connection)
    (.exchangeDeclare exchange "direct")
    (.queueDeclare queue false false false {})
    (.queueBind queue exchange queue)))

(declare publish)
(declare ack-message)

(defn publish-fn [channel exchange]
  (fn [queue msg]
    (let [msg-str (binding [*print-dup* true]
                    (pr-str msg))]
      (.basicPublish channel exchange queue
                     (MessageProperties/PERSISTENT_TEXT_PLAIN)
                     (.getBytes msg-str)))))

(defn rabbit-msgs 
  ([consumer]
   (if-let [next-msg (try
                       (.nextDelivery consumer)
                       (catch InterruptedException e
                         nil))]
     (lazy-seq
       (cons [(read-string (String. (.getBody next-msg)))
              next-msg]
             (rabbit-msgs consumer)))
     (rabbit-msgs consumer)))
  ([consumer msecs]
   (when-let [next-msg (.nextDelivery consumer msecs)]
     (lazy-seq
       (cons [(read-string (String. (.getBody next-msg)))
              next-msg]
             (rabbit-msgs consumer msecs))))))

(defn ack-message-fn [channel]
  (fn [msg]
    (.basicAck channel
               (.getDeliveryTag (.getEnvelope msg))
               false)))

(defn drain-queue [channel queue]
  (let [consumer (QueueingConsumer. channel)
        ack-message (ack-message-fn channel)]

    (.basicConsume channel queue, false, consumer)

    (dorun (map (fn [[s m]]
                  (ack-message m)
                  s)
                (rabbit-msgs consumer 100)))
    (.basicCancel channel (.getConsumerTag consumer))))

(deftest test-rabbit-publish-consume
         (let [exchange "conduit-exchange"
               queue "test-queue"]
           (with-open [connection (rabbitmq-connection "localhost" "/" "guest" "guest")
                       channel (rabbitmq-channel connection exchange queue)]
             (let [consumer (QueueingConsumer. channel)
                   ack-message (ack-message-fn channel)]

               (drain-queue channel queue)
               (dorun
                 (map (partial (publish-fn channel exchange) queue)
                      (range 50)))

               (.basicConsume channel queue, false, consumer)
               (is (= (range 50)
                      (map (fn [[s m]]
                             (ack-message m)
                             s)
                           (rabbit-msgs consumer 100))))))))

(with-arrow conduit
  (defn rabbitmq-handler [p queue]
    (apply a-select
           (apply concat
                  (seq
                    (dissoc (get-in p [:parts queue])
                                      :type)))))

  (defmethod conduit-run :rabbitmq [p queue channel exchange]
    (binding [ack-message (ack-message-fn channel)
              publish (publish-fn channel exchange)]
      (let [queue (str queue)
            consumer (QueueingConsumer. channel)]
        (.basicConsume channel queue, false, consumer)
        (dorun
          (a-run
            (a-seq (a-nth 0 (rabbitmq-handler p queue))
                   (a-nth 1 (a-arr ack-message)))
            (rabbit-msgs consumer 100)))))))

(defmacro def-rabbitmq [proc-name source proc-fn]
  `(def ~proc-name {:type :rabbitmq
                    :fn ~proc-fn
                    :parts {(str ~source) {:type :rabbitmq
                                           (str '~proc-name) ~proc-fn}}
                    :source (str ~source)
                    :id (str '~proc-name)}))

(def test-results (atom []))
(with-arrow conduit
            (def-rabbitmq test-rabbit 'test-queue
                          (a-arr (fn [n]
                                   (swap! test-results conj n)
                                   n))))

(with-arrow conduit
  (deftest test-rabbitmq-run
           (let [exchange "conduit-exchange"
                 queue "test-queue"]
             (with-open [connection (rabbitmq-connection "localhost" "/" "guest" "guest")
                         channel (rabbitmq-channel connection exchange queue)]

               (drain-queue channel queue)

               (dorun
                 (map (partial (publish-fn channel exchange) queue)
                      (map (fn [x] ["test-rabbit" x])
                           (range 10))))

               (reset! test-results [])
               (conduit-run test-rabbit "test-queue" channel exchange)
               (is (= (range 10)
                      @test-results))))))

(defmethod new-proc :rabbitmq [old-rabbitmq new-fn]
  (let [id (new-id)]
    (-> old-rabbitmq
      (assoc-in [:parts (:source old-rabbitmq) id] new-fn)
      (assoc :id id
             :fn new-fn))))

(defn publisher [p]
  (partial (fn [f x]
             (println "rabbitmq:" (:source p) [(:id p) x])
             (f x))
           (:fn p)))

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

#_(defmethod scatter-gather-fn :rabbitmq [p]
  (partial proc-fn-future (:fn p)))

(deftest test-new-rabbitmq
         (is (= {:fn :new-fn
                 :source :src
                 :type :rabbitmq
                 :id :g
                 :parts {:src {:g :new-fn, :first :f1}}}
                (binding [new-id (constantly :g)]
                  (new-proc {:source :src
                             :type :rabbitmq
                             :id :first
                             :parts {:src {:first :f1}}}
                            :new-fn)))))

(def-rabbitmq p1 'p1-source
        (comp vector dec))

(def-rabbitmq p2 'p2-source
        (comp vector (partial + 3)))

(def p3 (seq-proc p1 p2))
#_(println (a-run p3 [1 2 3]))

(def p4 (seq-proc p2 p1))
#_(println (a-run p4 [1 2 3]))

(run-tests)
