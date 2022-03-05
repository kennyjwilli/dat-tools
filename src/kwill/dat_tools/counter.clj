(ns kwill.dat-tools.counter
  "Auto-incrementing counter for Datomic."
  (:require
    [datomic.client.api :as d]))

(def schema
  "Required schema for the counter to function"
  [{:db/ident       :counter/key
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/value}
   {:db/ident       :counter/value
    :db/doc         "Current value of the counter."
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one}])

(defn get-id!
  [conn {:keys [key max-iter]
         :or   {max-iter 10}}]
  (let [db (d/db conn)
        lref [:counter/key key]
        fetchv #(:counter/value (d/pull db [:counter/value] lref))]
    (loop [iter 0]
      (let [current-value (fetchv)
            next-value (inc (or current-value 0))
            eid (if current-value lref "counter")
            tx-data (cond-> [[:db/cas eid :counter/value current-value next-value]]
                      (nil? current-value)
                      (conj {:db/id eid :counter/key key}))
            tx-report (try
                        (d/transact conn {:tx-data tx-data})
                        (catch Exception ex
                          (if (= :db.error/cas-failed (:db/error (ex-data ex)))
                            nil
                            (throw ex))))]
        (cond
          tx-report next-value
          (>= iter max-iter) (let [msg "Failed to allocate counter id."]
                               (throw (ex-info msg
                                        {:cognitect.anomalies/category :cognitect.anomalies/unavailable
                                         :cognitect.anomalies/message  msg
                                         :iter                         iter})))
          :else (recur (inc iter)))))))

(comment (sc.api/defsc 1)
  (def client (d/client {:server-type :dev-local
                         :system      "t"
                         :storage-dir :mem}))
  (d/create-database client {:db-name "t"})
  (def conn (d/connect client {:db-name "t"}))
  (d/transact conn {:tx-data schema})

  (get-id! conn {:key "e"})

  (d/transact conn {:tx-data [{:counter/key "b"}
                              [:db/cas [:counter/key "b"] :counter/value nil 1]]})
  (def e *e)

  (ex-data e)

  )
