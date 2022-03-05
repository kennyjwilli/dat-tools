(ns kwill.dat-tools.counter-test
  (:require
    [clojure.test :refer :all]
    [datomic.client.api :as d]
    [kwill.dat-tools.counter :as counter]))

(defn init!
  []
  (let [client (d/client {:server-type :dev-local
                          :system      "test"
                          :storage-dir :mem})
        db-name "test"
        _ (d/create-database client {:db-name db-name})]
    {:client client
     :conn   (d/connect client {:db-name db-name})
     :closef #(d/delete-database client {:db-name db-name})}))

(deftest get-id!-test
  (let [{:keys [conn closef]} (init!)]
    (try
      (d/transact conn {:tx-data counter/schema})
      (is (= 1 (counter/get-id! conn {:key "a"}))
        "first unique key starts at 1")
      (is (= 2 (counter/get-id! conn {:key "a"}))
        "second call increments")
      (is (= 1 (counter/get-id! conn {:key "b"}))
        "a new unique key")
      (finally (closef)))))
