(ns test-conduit-rabbitmq
  (:use conduit-rabbitmq :reload-all)
  (:use
     clojure.test
     [conduit :only [conduit a-run run-proc
                     conduit-map
                     new-proc conduit-seq
                     test-conduit
                     test-conduit-fn]]
     arrows)
  (:import
     [com.rabbitmq.client ConnectionParameters ConnectionFactory]))

(defn rabbitmq-connection [host vhost user password]
  "Create a simple rabbitmq connection."
  ;; for rabbitmq client 1.7.2
  (let [params (doto (ConnectionParameters.)
                 (.setVirtualHost "/")
                 (.setUsername "guest")
                 (.setPassword "guest"))
        factory (ConnectionFactory. params)]
    (.newConnection factory "localhost"))

  ;; for rabbitmq client 1.8.0
  #_(.newConnection
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
(def test-rabbit
  (rabbitmq-arr 'test-queue
                 (fn [x]
                   (swap! test-results conj x)
                   x)))

(with-arrow conduit
            (deftest test-rabbit-publish-consume
                     (dorun
                       (map (partial publish *queue*)
                            (range 50)))

                     (is (= (range 50)
                            (a-run (a-comp (msg-stream-proc *queue* 100)
                                          (a-arr (fn [m]
                                                   (ack-message m)
                                                   (read-msg m))))))))

            (deftest test-rabbitmq-run
                     (dorun
                       (map (partial publish *queue*)
                            (map (fn [x] [(:id test-rabbit) x])
                                 (range 10))))

                     (reset! test-results [])
                     (rabbitmq-run test-rabbit *queue* *channel* *exchange* 100)
                     (is (= (range 10)
                            @test-results)))

            (deftest test-new-proc
                     (let [new-rabbit (new-proc test-rabbit
                                                (fn this-fn [n]
                                                  (swap! test-results conj (inc n))
                                                  [[(inc n)] this-fn]))]
                       (dorun
                         (map (partial publish *queue*)
                              (map (fn [x] [(:id new-rabbit) x])
                                   (range 10))))

                       (reset! test-results [])
                       (rabbitmq-run new-rabbit *queue* *channel* *exchange* 100)
                       (is (= (range 1 11)
                              @test-results))))

            (deftest test-seq-proc
                     (let [new-rabbit (a-comp (a-arr inc)
                                             test-rabbit)]
                       (a-run (a-comp (conduit-seq (range 10))
                                     new-rabbit))

                       (reset! test-results [])
                       (rabbitmq-run new-rabbit *queue* *channel* *exchange* 100)
                       (is (= (range 1 11)
                              @test-results))))

            (deftest test-par-proc
                       (let [p1 (rabbitmq-arr *queue* inc)
                             p2 (rabbitmq-arr *queue* dec)
                             new-rabbit (a-all p1 p2)
                             thread-fn (fn [exchange queue]
                                         (with-open [connection (rabbitmq-connection "localhost" "/" "guest" "guest")
                                                     channel (.createChannel connection)]
                                           (rabbitmq-run new-rabbit queue channel exchange)))
                             remote-thread (doto (new Thread (partial thread-fn *exchange* *queue*))
                                             (.start))]

                         (try
                           (is (= (map vector
                                       (range 1 11)
                                       (range -1 9))
                                  (conduit-map new-rabbit (range 10))))
                           (finally
                             (Thread/sleep 500)
                             (.interrupt remote-thread)
                             (.join remote-thread 5000))))))

(deftest test-test
         (let [test-proc (with-arrow conduit
                                     (a-comp
                                       (a-arr inc)
                                       (a-rabbitmq "bogus-queue"
                                                   (a-arr (partial * 2)))))
               test-fn (test-conduit-fn test-proc)]
           
           (is (= [8 10 12 14]
                  (mapcat test-fn (range 3 7))))
           (is (= [8 10 12 14]
                  (conduit-map (test-conduit test-proc)
                               (range 3 7))))))


                                               
(run-tests)
