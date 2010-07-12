(ns net.intensivesystems.conduit-rabbitmq
  (:use
     clojure.test
     [clojure.pprint :only [pprint]]
     [net.intensivesystems.conduit :only [conduit conduit-run new-proc
                                          new-id seq-fn seq-proc a-run
                                          scatter-gather-fn proc-fn-future
                                          reply-proc]]
     net.intensivesystems.arrows)
  (:import
     [com.rabbitmq.client Connection ConnectionFactory Channel
      MessageProperties QueueingConsumer]))

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

(defn msg-list 
  ([consumer]
   (if-let [next-msg (try
                       (.nextDelivery consumer)
                       (catch InterruptedException e
                         nil))]
     (lazy-seq
       (cons [(read-string (String. (.getBody next-msg)))
              next-msg]
             (msg-list consumer)))
     (msg-list consumer)))
  ([consumer msecs]
   (when-let [next-msg (.nextDelivery consumer msecs)]
     (lazy-seq
       (cons [(read-string (String. (.getBody next-msg)))
              next-msg]
             (msg-list consumer msecs))))))

(defn rabbit-msgs
  ([queue] (msg-list (consumer queue)))
  ([queue msecs] (msg-list (consumer queue) msecs)))

(with-arrow conduit
  (defn rabbitmq-handler [p queue]
    (->> (dissoc (get-in p [:parts queue]) :type)
         seq
         (mapcat (fn [[x f]] [x {:fn f}]))
         (apply a-select)))

  (defmethod conduit-run :rabbitmq [p queue channel exchange]
    (binding [*channel* channel
              *exchange* exchange]
      (let [queue (str queue)]
        (dorun
          (a-run
            (a-seq (a-nth 0 (rabbitmq-handler p queue))
                   (a-nth 1 (a-arr ack-message)))
            (rabbit-msgs queue 100)))))))

(defmacro def-rabbitmq [proc-name source old-proc]
  `(def ~proc-name {:type :rabbitmq
                    :fn (:fn ~old-proc)
                    :parts {(str ~source) {:type :rabbitmq
                                           (str '~proc-name) (:fn ~old-proc)}}
                    :source (str ~source)
                    :id (str '~proc-name)}))

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

#_(defmethod scatter-gather-fn :rabbitmq [p]
  (fn [x]
    create-reply-queue
    (publish (:source p2) [x reply-queue])
    return future that waits on reply
    ))

(defmethod reply-proc :rabbitmq [p]
  (new-proc p
            (partial (fn this-fn [f [x reply-queue]]
                       (let [[new-x new-f] (f x)]
                         (publish reply-queue new-x)
                         [[] (partial this-fn new-f)]))
                     (:fn p))))

(defn rabbitmq-connection [host vhost user password]
  "Create a simple rabbitmq connection."
  (.newConnection
    (doto (ConnectionFactory.)
      (.setHost host)
      (.setVirtualHost vhost)
      (.setUsername user)
      (.setPassword password))))

(defn rabbitmq-test-fixture [f]
  (with-open [connection (rabbitmq-connection "localhost" "/" "guest" "guest")
              channel (.createChannel connection)]
    (binding [*channel* channel
              *exchange* "conduit-exchange"
              *queue* "test-queue"]
      (.exchangeDeclare channel *exchange* "direct")
      (declare-queue *queue*)
      (purge-queue *queue*)
      (f))))

(use-fixtures :each rabbitmq-test-fixture)

(def test-results (atom []))
(with-arrow conduit
            (def-rabbitmq test-rabbit 'test-queue
                          (a-arr (fn [n]
                                   (swap! test-results conj n)
                                   n))))

(deftest test-rabbit-publish-consume
         (dorun
           (map (partial publish *queue*)
                (range 50)))

         (is (= (range 50)
                (map (fn [[s m]]
                       (ack-message m)
                       s)
                     (rabbit-msgs *queue* 100)))))

(deftest test-rabbitmq-run
         (dorun
           (map (partial publish *queue*)
                (map (fn [x] ["test-rabbit" x])
                     (range 10))))

         (reset! test-results [])
         (conduit-run test-rabbit *queue* *channel* *exchange*)
         (is (= (range 10)
                @test-results)))

(deftest test-new-proc
        (let [new-rabbit (new-proc test-rabbit (fn this-fn [n]
                                                 (swap! test-results conj (inc n))
                                                 [[(inc n)] this-fn]))]
          (dorun
            (map (partial publish *queue*)
                 (map (fn [x] [(:id new-rabbit) x])
                      (range 10))))

          (reset! test-results [])
          (conduit-run new-rabbit *queue* *channel* *exchange*)
          (is (= (range 1 11)
                 @test-results))))

(deftest test-seq-proc
        (let [new-rabbit (with-arrow conduit
                                     (a-seq (a-arr inc)
                                            test-rabbit))]
          (a-run new-rabbit (range 10))

          (reset! test-results [])
          (conduit-run new-rabbit *queue* *channel* *exchange*)
          (is (= (range 1 11)
                 @test-results))))

(run-tests)
