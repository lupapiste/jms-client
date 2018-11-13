(ns lupapiste-jms.client
  "Thin Clojure wrapper for JMS client-side usage."
  (:require [clojure.string :as s])
  (:import [javax.jms Connection ConnectionFactory Session
                      Destination ExceptionListener MessageListener
                      Queue Topic
                      JMSContext JMSProducer
                      Message MessageProducer MessageConsumer
                      BytesMessage ObjectMessage TextMessage]
           [clojure.lang Fn]))

;;;; Connections
;;;; ===================================================================================================================

(defn create-connection
  "Creates connection to given connection factory. Doesn't start the connection.
   Options map:
         - :username
         - :password
         - :ex-listener - sets exception listener for connection, see exception-listener."
  (^Connection [connection-factory] (create-connection connection-factory {}))
  (^Connection [^ConnectionFactory connection-factory {:keys [username password ex-listener]}]
   (let [conn (if (s/blank? username)
                (.createConnection connection-factory)
                (.createConnection connection-factory username password))]
     (when ex-listener
       (.setExceptionListener conn ex-listener))
     conn)))

;;;; Sessions and Transactions
;;;; ===================================================================================================================

(defn create-session
  "JMS 2.0 spec'd session creator."
  (^Session [^Connection conn]
   (create-session conn JMSContext/AUTO_ACKNOWLEDGE))
  (^Session [^Connection conn session-mode]
   (.createSession conn session-mode)))

(defn create-transacted-session ^Session [conn]
  (create-session conn Session/SESSION_TRANSACTED))

(defn commit [^Session session] (.commit session))
(defn rollback [^Session session] (.rollback session))

;;;; Contexts
;;;; ===================================================================================================================

(defn create-context
  "Creates JMSContext (JMS 2.0).
  Optional parameters as map:
     - :username
     - :password
     - :session-mode (see JMSContext API docs)
     - :ex-listener (ExceptionListener)"
  (^JMSContext [^ConnectionFactory cf]
   (create-context cf {}))
  (^JMSContext [^ConnectionFactory cf {:keys [session-mode username password ex-listener]
                                       :or   {session-mode JMSContext/AUTO_ACKNOWLEDGE}}]
   (let [ctx (if (s/blank? username)
               (.createContext cf session-mode)
               (.createContext cf username password session-mode))]
     (when ex-listener
       (.setExceptionListener ctx ex-listener))
     ctx)))

;;;; Messages
;;;; ===================================================================================================================

(defprotocol MessageFactory
  "Protocol for creating specific javax.jms.Message subtypes."
  (create-byte-message ^BytesMessage [factory ^bytes bytes] "Create a javax.jms.BytesMessage from the byte array.")
  (create-text-message ^TextMessage [factory ^String s] "Create a javax.jms.TextMessage from the String."))

(extend-protocol MessageFactory
  Session
  (create-byte-message [factory bytes] (doto (.createBytesMessage factory) (.writeBytes bytes)))
  (create-text-message [factory s] (.createTextMessage factory s))

  JMSContext
  (create-byte-message [factory bytes] (doto (.createBytesMessage factory) (.writeBytes bytes)))
  (create-text-message [factory s] (.createTextMessage factory s)))

(defprotocol MessageCreator
  "Protocol for creating instance of javax.jms.Message."
  (create-message [data message-factory] "Create Message depending on type of data."))

(extend-protocol MessageCreator
  (type (byte-array 0))
  (create-message [data factory] (create-byte-message factory data))

  String
  (create-message [data factory] (create-text-message factory data)))

(defn byte-message-as-array ^bytes [^BytesMessage m]
  (let [data (byte-array (.getBodyLength ^BytesMessage m))]
    (.readBytes ^BytesMessage m data)
    data))

(defprotocol MessageContent
  (message-content [msg]))

(extend-protocol MessageContent
  BytesMessage
  (message-content ^bytes [msg] (byte-message-as-array msg))

  TextMessage
  (message-content ^String [msg] (.getText msg))

  ObjectMessage
  (message-content ^Object [msg] (.getObject msg)))

(defn set-message-properties
  "Sets given properties (a map) into Message.
  Returns possibly mutated msg."
  ^Message [^Message msg properties]
  (doseq [[^String k v] properties]
    (condp instance? v
      Boolean (.setBooleanProperty msg k v)
      Byte (.setByteProperty msg k v)
      Double (.setDoubleProperty msg k v)
      Float (.setFloatProperty msg k v)
      Integer (.setIntProperty msg k v)
      Long (.setLongProperty msg k v)
      Short (.setShortProperty msg k v)
      String (.setStringProperty msg k v)
      (.setObjectProperty msg k v)))
  msg)

;;;; Queues and Topics
;;;; ===================================================================================================================

(defn create-queue ^Queue [^Session session name]
  (.createQueue session name))

(defn create-topic ^Topic [^Session session name]
  (.createTopic session name))

;;;; Producers
;;;; ===================================================================================================================

(defn create-jms-producer
  "Creates JMS 2.0 defined JMSProducer.
  Optional parameters as map:
     - :delivery-mode
     - :delivery-delay
     - :ttl
     - :correlation-id
     - :reply-to"
  (^JMSProducer [^JMSContext ctx]
   (.createProducer ctx))
  (^JMSProducer [^JMSContext ctx {:keys [delivery-mode delivery-delay ttl correlation-id reply-to]}]
   (let [producer (.createProducer ctx)]
     (when delivery-mode
       (.setDeliveryMode producer delivery-mode))
     (when delivery-delay
       (.setDeliveryDelay producer delivery-delay))
     (when ttl
       (.setTimeToLive producer ttl))
     (when correlation-id
       (.setJMSCorrelationID producer correlation-id))
     (when reply-to
       (.setJMSReplyTo producer reply-to))
     producer)))

(defn create-producer
  "Creates message producer for given destination (JMS 1.1)"
  ^MessageProducer [^Session session ^Destination destination]
  (.createProducer session destination))

(defn producer-fn
  "Returns function that takes one parameter: data to be sent.
  The data is sent via producer's send method.
  message-fn is called with raw data to create needed javax.jms.Message.
  Thus message-fn should create a javax.jms.Message which can be send with producer."
  [^MessageProducer producer message-fn]
  (fn [data] (.send producer (message-fn data))))

(defn text-message-producer
  "Creates producer function, which sends given data as a TextMessage to producer's destination."
  [^Session session producer]
  (producer-fn producer (partial create-text-message session)))

(defn byte-message-producer
  "Creates producer function, which sends given bytes data as a ByteMessage to producer's destination."
  [^Session session producer]
  (producer-fn producer (partial create-byte-message session)))

;;;; Consumers
;;;; ===================================================================================================================

(defn create-consumer ^MessageConsumer [^Session session ^Destination destination]
  (.createConsumer session destination))

(defn message-listener
  "Creates a MessageListener instance. Given callback function is fed with `message-content` of javax.jms.Message."
  ^MessageListener [cb]
  (reify MessageListener
    (onMessage [_ m] (cb (message-content m)))))

(defn exception-listener
  "Implements ExceptionListener. Passes exception to given listener-fn."
  ^ExceptionListener [listener-fn]
  (reify ExceptionListener
    (onException [_ e] (listener-fn e))))

(defn set-listener
  "Set a MessageListener for consumer.
  Listener-fn can be instance of MessageListener or a regular Clojure function.
  For regular function, an MessageListener instance is created and the function is fed with data from javax.jms.Message.
  Given consumer is returned."
  ^MessageConsumer [^MessageConsumer consumer listener-fn]
  (let [listener (condp instance? listener-fn
                   MessageListener listener-fn
                   Fn (message-listener listener-fn))]
    (.setMessageListener consumer listener)
    consumer))

(defn listen
  "Start consuming destination with listener-fn, which can be a Clojure function or instance of javax.jms.MessageListener.
  Returns created consumer."
  ^MessageConsumer [^Session session destination listener-fn]
  (set-listener (.createConsumer session destination) listener-fn))

(defn send-with-context
  "JMS 2.0 style producing. Creates context, sends data to destination and finally closes context.
   Message is created by default with 'create-message'.
   'create-message' function can be extended for other types than String and bytes.
   Also message creating function can be passed as :message-fn option.
   That function is invoked with two params: data and the created context.
   Options map is passed to create-context and create-jms-producer, see them for usable keys."
  [^ConnectionFactory cf ^Destination dest data & [options-map]]
  (with-open [ctx (create-context cf options-map)]
    (let [^JMSProducer prod (create-jms-producer ctx options-map)
          message-fn (get options-map :message-fn create-message)
          ^Message msg (message-fn data ctx)]
      (.send prod dest msg)
      nil)))
