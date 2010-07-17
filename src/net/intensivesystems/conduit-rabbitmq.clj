(ns net.intensivesystems.conduit-rabbitmq
  (:use
     clojure.test
     [clojure.pprint :only [pprint]]
     [net.intensivesystems.conduit :only [conduit conduit-run new-proc
                                          new-id seq-fn seq-proc a-run
                                          scatter-gather-fn
                                          reply-proc]]
     net.intensivesystems.arrows)
  (:import
     [java.util UUID]
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
    (println "msg-str:" msg-str)
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

(defn get-message 
  ([consumer]
   (try
     (.nextDelivery consumer)
     (catch InterruptedException e
       nil)))
  ([consumer msecs]
   (.nextDelivery consumer msecs)))

(defn msg-list 
  ([consumer]
   (if-let [next-msg (get-message consumer)]
     (lazy-seq
       (cons [
              next-msg]
             (msg-list consumer)))
     (msg-list consumer)))
  ([consumer msecs]
   (when-let [next-msg (get-message consumer msecs)]
     (lazy-seq
       (cons [(read-string (String. (.getBody next-msg)))
              next-msg]
             (msg-list consumer msecs))))))

(defn rabbit-msgs
  ([queue] (msg-list (consumer queue)))
  ([queue msecs] (msg-list (consumer queue) msecs)))

(defn rabbit-msg
  ([queue]
   (let [next-msg (get-message (consumer queue))]
     [(read-string (String. (.getBody next-msg)))
      next-msg]))
  ([queue msecs]
   (let [next-msg (get-message (consumer queue) msecs)]
     [(read-string (String. (.getBody next-msg)))
      next-msg])))

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
            (rabbit-msgs queue 100))))))
            
  (defn rabbitmq-arr [source f]
    (let [id (new-id)
          proc-fn (fn this-fn [x]
                    (println "x:" x)
                    [[(f x)] this-fn])
          #_(:fn (a-arr f))
          source (str source)]
      {:type :rabbitmq
       :fn proc-fn
       :parts {source {:type :rabbitmq
                             id proc-fn}}
       :source source
       :id id})))

#_(defmacro def-rabbitmq [proc-name source old-proc]
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

(defmethod scatter-gather-fn :rabbitmq [p]
  (fn this-fn [x]
    (let [reply-queue (str (UUID/randomUUID))]
      (.queueDeclare *channel* reply-queue false false false {})
      (.queueBind *channel* reply-queue *exchange* reply-queue)
      (println "scatter:" (:source p) [(:id p) [x reply-queue]]) (flush)
      (publish (:source p) [(:id p) [x reply-queue]])
      (fn []
        (println "waiting on:" reply-queue)
        [(rabbit-msg reply-queue) this-fn]))))

(defmethod reply-proc :rabbitmq [p]
  (with-arrow conduit
              (a-seq (a-nth 0 p)
                     {:fn (fn this-fn [[x reply-queue]]
                            (println "response:" reply-queue x)
                            (publish reply-queue x)
                            [[] this-fn])})))

(defn rabbitmq-connection [host vhost user password]
  "Create a simple rabbitmq connection."
  (.newConnection
    (doto (ConnectionFactory.)
      (.setHost host)
      (.setVirtualHost vhost)
      (.setUsername user)
      (.setPassword password))))

(declare *connection*)
(declare *queue*)
(defn rabbitmq-test-fixture [f]
  (with-open [connection (rabbitmq-connection "localhost" "/" "guest" "guest")
              channel (.createChannel connection)]
    (binding [*connection* connection
              *channel* channel
              *exchange* "conduit-exchange"
              *queue* "test-queue"]
      (.exchangeDeclare channel *exchange* "direct")
      (declare-queue *queue*)
      (purge-queue *queue*)
      (f))))

(use-fixtures :each rabbitmq-test-fixture)

(def test-results (atom []))
#_(def test-rabbit (rabbitmq-arr 'test-queue
                               (fn [n]
                                 (swap! test-results conj n)
                                 n)))

#_(deftest test-rabbit-publish-consume
         (dorun
           (map (partial publish *queue*)
                (range 50)))

         (is (= (range 50)
                (map (fn [[s m]]
                       (ack-message m)
                       s)
                     (rabbit-msgs *queue* 100)))))

#_(deftest test-rabbitmq-run
         (dorun
           (map (partial publish *queue*)
                (map (fn [x] [(:id test-rabbit) x])
                     (range 10))))

         (reset! test-results [])
         (conduit-run test-rabbit *queue* *channel* *exchange*)
         (is (= (range 10)
                @test-results)))

#_(deftest test-new-proc
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

#_(deftest test-seq-proc
        (let [new-rabbit (with-arrow conduit
                                     (a-seq (a-arr inc)
                                            test-rabbit))]
          (a-run new-rabbit (range 10))

          (reset! test-results [])
          (conduit-run new-rabbit *queue* *channel* *exchange*)
          (is (= (range 1 11)
                 @test-results))))

(defn bogus [f xs]
  (println "first x:" (first xs))
  (let [[new-x new-f] (f (ffirst xs))]
    (if (empty? new-x)
      (recur new-f ((second xs)))
      (lazy-seq
        (cons (first new-x)
              (bogus new-f ((second xs))))))))

(deftest test-par-proc
         (let [new-rabbit (with-arrow conduit
                                      (a-all (rabbitmq-arr "test-queue" inc)
                                             (rabbitmq-arr "test-queue" dec)))
               thread-fn (fn [exchange]
                           (with-arrow conduit
                                       (with-open [connection (rabbitmq-connection "localhost" "/" "guest" "guest")
                                                   channel (.createChannel connection)]
                                         (binding [*connection* connection
                                                   *channel* channel
                                                   *exchange* "conduit-exchange"]
                                           (let [queue "test-queue"]
                                             (dorun
                                               (bogus
                                                 (:fn (a-seq (a-nth 0 (rabbitmq-handler new-rabbit queue))
                                                             (a-nth 1 (a-arr ack-message))))
                                                 (rabbit-msgs queue 100))))))))
              remote-thread (doto (new Thread (partial thread-fn *exchange*))
                              (.start))
              ]

          (try
            (a-run new-rabbit (range 10))
            (reset! test-results [])
            (is (= (map (fn [x y] [x y])
                        (range 1 11)
                        (range -1 9))
                   @test-results))

            (finally
              (Thread/sleep 500)
              (.interrupt remote-thread)
              (println "waiting...")
              (.join remote-thread 5000)
              (println "done waiting")))))

(run-tests)
