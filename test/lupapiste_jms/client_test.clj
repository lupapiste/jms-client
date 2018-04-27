(ns lupapiste-jms.client-test
  (:require [clojure.test :refer :all]
            [lupapiste-jms.client :refer :all])
  (:import (javax.jms Connection ConnectionFactory Session Destination
                      MessageProducer MessageConsumer
                      TextMessage BytesMessage)))

(def state (atom {:started    false
                  :closed     true
                  :connection nil
                  :sessions   []
                  :consumers  []
                  :producers  []
                  :messages {:produced []
                             :consumed []}}))

(def QueueImpl (reify Destination (toString [_] "lupapiste-jms.test.queue")))
(defn text-message [text]
  (reify TextMessage
    (getText [_]
      text)))

(defn producer [dest]
  (reify MessageProducer
    (send [_ msg]
      (swap! state update-in [:messages :produced] conj {:msg msg :destination dest}))))

(defn consumer [dest]
  (reify MessageConsumer
    (getMessageListener [_] )
    (setMessageListener [_ listener]
      ; mimic 'consuming' of messages
      (doseq [msg (get-in @state [:messages :produced])
              :when (= dest (:destination msg))]
        (swap! state update-in [:messages :produced] #(remove (fn [old-msg] (= old-msg msg)) %))
        (.onMessage listener (:msg msg))))))

(defn session []
  (reify Session
    (createProducer [this dest]
      (let [prod (producer dest)]
        (swap! state update :producers conj {:producer prod :session this :destination dest})
        prod))
    (createConsumer [this dest]
      (let [consumer (consumer dest)]
        (swap! state update :consumers conj {:session this :destination dest :consumer consumer})
        consumer))
    (createTextMessage [_ msg]
      (text-message msg))))

(def ConnectionImpl
  (reify Connection
    (start [_] (swap! state assoc :started true :closed false))
    (stop [_]  (swap! state assoc :started false :closed false))
    (close [_] (swap! state assoc :started false :closed true))
    (createSession [_ mode]
      (let [sess (session)]
        (swap! state update :sessions conj {:session sess :mode mode})
        sess))))

(def FactoryImpl
  (reify ConnectionFactory
    (createConnection [_]
      (swap! state assoc :connection (with-meta ConnectionImpl {:username nil :password nil}))
      ConnectionImpl)
    (createConnection [_ username password]
      (swap! state assoc :connection (with-meta ConnectionImpl {:username username :password password}))
      ConnectionImpl)))

(deftest connection-test
  (let [conn (create-connection FactoryImpl)]
    (is (instance? Connection conn) "javax.jms.Connection")
    (is (nil? (:username (meta (:connection @state)))) "Username is not set")
    (is (nil? (:password (meta (:connection @state)))) "Password is not set")))

(deftest connection-with-credentials-test
  (let [conn (create-connection FactoryImpl {:username "foo" :password "test"})]
    (is (instance? Connection conn) "javax.jms.Connection")
    (is (not (nil? (:connection @state))) "Connection is not nil")
    (is (= "foo" (:username (meta (:connection @state)))) "Username is set")
    (is (= "test" (:password (meta (:connection @state)))) "Password is set")))

(deftest session-test
  (is (zero? (count (:sessions @state))) "No sessions yet")
  (let [sess (create-session (:connection @state))]
    (is (instance? Session sess))
    (is (not (zero? (count (:sessions @state)))))))

(deftest producer-test
  (is (zero? (count (:producers @state))) "No producers yet")
  (let [test-session (-> (:sessions @state) (first) :session)
        prod (create-producer test-session QueueImpl)
        txt-producer (text-message-producer test-session prod)]
    (is (instance? MessageProducer prod))
    (is (not (zero? (count (:producers @state)))) "Producer registered")
    (is (zero? (count (get-in @state [:messages :produced]))) "No messages yet")
    (txt-producer "test message")
    (let [msgs (get-in @state [:messages :produced])]
      (is (= 1 (count msgs)) "Message produced")
      (is (= "test message" (.getText (:msg (first msgs)))) "Message text is correct"))))

(defn message-listener-fn [data]
  (swap! state update-in [:messages :consumed] conj data))

(deftest consumer-test
  (is (zero? (count (:consumers @state))) "No consumers yet")
  (is (zero? (count (get-in @state [:messages :consumed]))) "No received messages yet")
  (is (= 1   (count (get-in @state [:messages :produced]))) "One message produced")
  (let [test-session (-> (:sessions @state) (first) :session)
        listener (listen test-session QueueImpl message-listener-fn)]
    (is (instance? MessageConsumer listener))
    (is (not (zero? (count (:consumers @state)))) "Consumer registered")
    (is (= 1 (count (get-in @state [:messages :consumed]))) "One message was received by listener ('consumed')")
    (is (zero? (count (get-in @state [:messages :produced]))) "No produced messages anymore")))

(deftest close-connection-test
  (.close (:connection @state))
  (is (true? (:closed @state)) "Connection closed"))

(def test-message-session
  (reify Session
    (createBytesMessage [_]
      (reify BytesMessage
        (writeBytes [_ _] "discarded")
        (readBytes [_ result]
          1)))
    (createTextMessage [_ data]
      (reify TextMessage (getText [_] data)))))

(deftest message-creator-protocol-test
  (let [text-message (create-message "TextMessage data" test-message-session)
        byte-message (create-message (.getBytes "this string is discarded") test-message-session)]
    (is (instance? TextMessage text-message)
        "TextMessage OK")
    (is (= "TextMessage data" (.getText text-message))
        "Correct data")
    (is (instance? BytesMessage byte-message)
        "ByteMessage OK")
    (is (= 1 (.readBytes byte-message nil))
        "Bytes 'data' correct")
    (is (thrown? IllegalArgumentException (create-message {:invalid :map} test-message-session))
        "No protocol implementation for map")))
