(ns kwill.dat-tools.lock-test
  (:require
    [clojure.test :refer :all]
    [kwill.dat-tools.lock :as lock]
    [datomic.client.api :as d])
  (:import (clojure.lang ExceptionInfo)))

(defn init!
  []
  (let [client (d/client {:server-type :dev-local
                          :system      "test"
                          :storage-dir :mem})
        db-name "test"
        _ (d/delete-database client {:db-name db-name})
        _ (d/create-database client {:db-name db-name})]
    {:client client
     :conn   (d/connect client {:db-name db-name})}))

(deftest lock-test
  ;(lock/revert-transact {})
  (let [{:keys [conn]} (init!)
        env (#'lock/new-env)
        acquire #(lock/acquire conn (merge {:env env
                                            :divert? true} %))]
    (d/transact conn {:tx-data lock/schema})

    (testing "common lock ops"
      (with-open [l-a (acquire {:id "a"})]
        (is (= "a" (:id l-a)))
        (is (thrown? ExceptionInfo (acquire {:id "a"}))
          "acquiring same lock twice w/o release & before expiry throws"))
      (with-open [l-a (acquire {:id "a"})]
        (is (= "a" (:id l-a))
          "require lock after implicit release via .close")
        (is (= [[:db/cas [:dat-tools.lock/id "a"] :dat-tools.lock/fencing-token 0 0]]
              (#'lock/get-all-lease-tx-data env))
          "lease tx-data include acquired lock :fence-tx-data"))
      (is (= [] (#'lock/get-all-lease-tx-data env))
        "no lease tx-data after lock is closed"))

    (testing "lock past expiry"
      (with-open [l-a (acquire {:id           "a"
                                :get-date-now (constantly #inst"2020")})]
        (with-open [l-a' (acquire {:id           "a"
                                   :get-date-now (constantly #inst"2021")})]
          (is (= "a" (:id l-a'))))))

    (testing "fencing"
      (with-open [l-a (acquire {:id           "a"
                                :divert?      false
                                :get-date-now (constantly #inst"2020")})]
        ;; Stop the heartbeat
        ((get-in l-a [:_heartbeat ::lock/closef]))
        ;; Acquire lock 2nd time since l-a will be considered expired.
        (with-open [l-a' (acquire {:id           "a"
                                   :get-date-now (constantly #inst"2021")
                                   ;; we don't divert with this lock since we don't want our tx-data to include
                                   ;; the new fence token
                                   :divert?      false})]
          (is (thrown-with-msg? ExceptionInfo #"Compare failed"
                (d/transact conn {:tx-data (:fence-tx-data l-a)}))
            "transacting with expired fence token throws"))))))
