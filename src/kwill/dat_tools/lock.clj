(ns kwill.dat-tools.lock
  "Use Datomic as a distributed lock."
  (:require
    [datomic.client.api :as d]
    [datomic.client.api.async :as d.a])
  (:import (java.io Closeable)
           (java.time Duration)
           (java.util.concurrent ScheduledThreadPoolExecutor TimeUnit)))

(def schema
  [{:db/ident       :dat-tools.lock/id
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/value}
   {:db/ident       :dat-tools.lock/last-heartbeat-date
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/noHistory   true}
   {:db/ident       :dat-tools.lock/fencing-token
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one
    :db/noHistory   true}])

(def default-heartbeat-interval-ms (.toMillis (Duration/ofSeconds 10)))
(def default-max-heartbeat-duration-ms
  (+ default-heartbeat-interval-ms
    (int (/ default-heartbeat-interval-ms 2))))

(defonce ^:private *transact-fns (atom nil))

(defn get-date-now [] (java.util.Date.))

(defn- transact
  "Wrapper transact function to ensure the lock code itself only ever uses a raw
  transact (not munged with fence tx-data)."
  [conn argm]
  ((or (get @*transact-fns `d/transact) d/transact) conn argm))

(defn new-heartbeat
  [conn {:keys [id get-date-now interval-ms fence-tx-data]
         :or   {interval-ms  default-heartbeat-interval-ms
                get-date-now get-date-now}}]
  (let [exec (ScheduledThreadPoolExecutor. 1)
        _ (.scheduleAtFixedRate exec
            ^Runnable (fn []
                        (transact conn {:tx-data (into [{:db/id                              [:dat-tools.lock/id id]
                                                         :dat-tools.lock/last-heartbeat-date (get-date-now)}]
                                                   fence-tx-data)}))
            interval-ms interval-ms TimeUnit/MILLISECONDS)]
    {::executor exec
     ::closef   #(.shutdownNow exec)}))

(defn- new-env
  []
  {::*id->lease (atom {})})

(defonce ^:private default-env (new-env))

(defn- get-all-lease-tx-data
  [env]
  (into []
    (comp
      (map val)
      (filter :divert?)
      (mapcat :fence-tx-data))
    @(::*id->lease env)))

(defn divert-transact
  "Diverts calls to transact to ensure `fence-tx-data` is always present in the
  :tx-data by changing the var root. Can be undone with [[revert-transact]]."
  [{:keys [env]}]
  (swap! *transact-fns
    (fn [fns]
      (or fns {`d/transact   d/transact
               `d.a/transact d.a/transact})))
  (let [alter-fn (fn [transact]
                   (fn [conn argm]
                     (transact conn (update argm :tx-data into (get-all-lease-tx-data env)))))]
    (alter-var-root #'d/transact alter-fn)
    (alter-var-root #'d.a/transact alter-fn))
  true)

(defn revert-transact
  "Complement to [[divert-transact]] by undoing the global var mutation."
  [_]
  (let [{d-transact  `d/transact
         da-transact `d.a/transact
         :or         {d-transact  d/transact
                      da-transact d.a/transact}} @*transact-fns]
    (alter-var-root #'d/transact (constantly d-transact))
    (alter-var-root #'d.a/transact (constantly da-transact))
    true))

(defrecord Lease [_closef]
  Closeable
  (close [_] (_closef)))

(defn release
  "Release the lock identified by `id`."
  [conn {:keys [id env]}]
  (swap! (::*id->lease env) dissoc id)
  (transact conn {:tx-data [[:db/retractEntity [:dat-tools.lock/id id]]]})
  true)

(defn acquire
  "Acquires a lease on the lock identified by `id`. Throws if the lease cannot be
  acquired for one of the following reasons:
    1. The lock is already acquired.
    2. Datomic client exception.

  Take an argument map with the following keys:
    - max-heartbeat-duration-ms: Max number of millis before a call to acquire can
      consider an existing lease expired. Defaults to 15ms.
    - divert?: True if all calls to d/transact or d.a/transact should be forced
      to include the fence-tx-data. Defaults to false.
    - extra-data: A map of extra data to add to the lock map. All attributes must
      be fully managed by the application.

  Returns a Closable with the following keys:
    - fence-tx-data: Datomic tx-data that should be included in every Datomic
      transaction while the lease is held. Failure to do so can result in data
      corruption due to process pauses."
  ([conn argm] (acquire nil conn argm))
  ([_ conn {:keys [id extra-data max-heartbeat-duration-ms get-date-now divert? env]
            :or   {max-heartbeat-duration-ms default-max-heartbeat-duration-ms
                   get-date-now              get-date-now
                   env                       default-env}}]
   (let [lref [:dat-tools.lock/id id]
         {:dat-tools.lock/keys [last-heartbeat-date
                                fencing-token]} (d/pull (d/db conn)
                                                  [:dat-tools.lock/fencing-token
                                                   :dat-tools.lock/last-heartbeat-date]
                                                  lref)
         date-now (get-date-now)
         heartbeat-diff-ms (when last-heartbeat-date (- (inst-ms date-now) (inst-ms last-heartbeat-date)))]
     (if (or
           ;; lock does not exist
           (nil? heartbeat-diff-ms)
           ;; lock is past expiry
           (> heartbeat-diff-ms max-heartbeat-duration-ms))
       (let [new-fencing-token (or (some-> fencing-token inc) 0)
             tx-data (if heartbeat-diff-ms
                       (cond->
                         [[:db/cas lref :dat-tools.lock/last-heartbeat-date last-heartbeat-date date-now]
                          [:db/cas lref :dat-tools.lock/fencing-token fencing-token new-fencing-token]]
                         extra-data
                         (conj (assoc extra-data :db/id lref)))
                       [(assoc extra-data
                          :db/id "lock"
                          :dat-tools.lock/id id
                          :dat-tools.lock/fencing-token new-fencing-token
                          :dat-tools.lock/last-heartbeat-date date-now)
                        [:db/cas "lock" :dat-tools.lock/id nil id]])
             fence-tx-data [[:db/cas lref :dat-tools.lock/fencing-token new-fencing-token new-fencing-token]]
             base-lease-map {:id            id
                             :divert?       divert?
                             :fence-tx-data fence-tx-data}
             tx-report (try
                         (swap! (::*id->lease env)
                           (fn [id->lease]
                             (transact conn {:tx-data tx-data})
                             (when divert? (divert-transact {:env env}))
                             (assoc id->lease id base-lease-map)))
                         (catch Exception ex
                           (let [{:db/keys [error]} (ex-data ex)]
                             (if (= :db.error/unique-conflict error)
                               (throw (ex-info (str "Lock id \"" id "\" is already acquired.") {:id id} ex))
                               (throw ex)))))
             heartbeat (new-heartbeat conn {:id           id
                                            :get-date-now get-date-now
                                            :interval-ms  1000})]
         (map->Lease
           (assoc base-lease-map
             :_heartbeat heartbeat
             :_closef (fn []
                        ;; Stop heartbeat executor
                        ((::closef heartbeat))
                        ;; Release the lock
                        (release conn {:id id :env env})))))
       (throw (ex-info (str "Lock id \"" id "\" is already acquired.")
                {:id           id
                 :ms-to-expiry (- max-heartbeat-duration-ms heartbeat-diff-ms)}))))))

(comment (sc.api/defsc 2)
  (require 'sc.api)
  (def client (d/client {:server-type :dev-local
                         :system      "t"
                         :storage-dir :mem}))
  (d/create-database client {:db-name "t"})
  (def conn (d/connect client {:db-name "t"}))
  (d/transact conn {:tx-data schema})

  (def lease (acquire conn {:id "b"}))
  (d/transact conn {:tx-data (:fence-tx-data lease)})
  (acquire conn {:id "b"})
  (release conn {:id "b"})
  (.close lease)
  (.close *1)
  (ex-message *e)
  (ex-data *e)

  (d/pull (d/db conn) '[*] [:dat-tools.lock/id "b"])



  (d/transact conn {:tx-data [{:db/id                              79164837199947
                               :dat-tools.lock/last-heartbeat-date #inst"2020"}]})
  (d/transact conn {:tx-data [
                              [:db/cas 79164837199947 :dat-tools.lock/last-heartbeat-date #inst"2020" nil]]})

  (let [id "a"]
    (d/transact conn {:tx-data [{:db/id             "lock"
                                 :dat-tools.lock/id id}
                                [:db/cas "lock" :dat-tools.lock/id nil id]]}))

  (get-id! conn {:key "e"})

  (d/transact conn {:tx-data [{:counter/key "b"}
                              [:db/cas [:counter/key "b"] :counter/value nil 1]]})
  (def e *e)
  (ex-data e)

  (ex-data (ex-info "a" {:a ""} e))

  )
