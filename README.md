# dat-tools

A collection of utilities for working with Datomic.

## Installation

Add to your `deps.edn`:

```clojure
{:deps
 {dev.kwill/dat-tools {:git/url "https://github.com/kennyjwilli/fluxjure.git" :sha "..."}}}
```

## Features

### Auto-incrementing Counter

The `kwill.dat-tools.counter` namespace provides an auto-incrementing counter backed by Datomic.

#### Schema

Add the counter schema to your Datomic database:

```clojure
(require '[kwill.dat-tools.counter :as counter])
(require '[datomic.client.api :as d])

(d/transact conn {:tx-data counter/schema})
```

#### Usage

Get the next ID from a named counter:

```clojure
;; Get the next ID for the "user-id" counter
(counter/get-id! conn {:key "user-id"})
;; => 1

;; Call again to get the next ID
(counter/get-id! conn {:key "user-id"})
;; => 2

;; Use a different key for a separate counter
(counter/get-id! conn {:key "order-id"})
;; => 1
```

Options:
- `:key` - Required. The unique key identifying this counter.
- `:max-iter` - Optional. Maximum iterations to attempt when there's contention (defaults to 10).

### Distributed Lock

The `kwill.dat-tools.lock` namespace provides a distributed lock mechanism using Datomic.

#### Schema

Add the lock schema to your Datomic database:

```clojure
(require '[kwill.dat-tools.lock :as lock])
(require '[datomic.client.api :as d])

(d/transact conn {:tx-data lock/schema})
```

#### Usage

Acquire and release locks:

```clojure
(require '[kwill.dat-tools.lock :as lock])

;; Acquire a lock
(def my-lock (lock/acquire conn {:id "process-orders"}))

;; The lock implements java.io.Closeable
(try
  ;; Perform work while holding the lock
  (process-orders)
  (finally
    ;; Release the lock
    (.close my-lock)))

;; Or use with-open for automatic cleanup
(with-open [my-lock (lock/acquire conn {:id "process-orders"})]
  (process-orders))
```

Advanced usage with fencing tokens:

```clojure
;; Enable fence token enforcement for all transactions
(with-open [my-lock (lock/acquire conn {:id "process-orders" :divert? true})]
  ;; All d/transact calls will include fencing tokens automatically
  (d/transact conn {:tx-data [...]})
  (process-orders))

;; Manual fence token inclusion
(with-open [my-lock (lock/acquire conn {:id "process-orders"})]
  ;; Include fence token explicitly in your transactions
  (d/transact conn {:tx-data (into (:fence-tx-data my-lock) 
                                 [{:order/id "123" :order/status :processing}])}))
```

Lock options:
- `:id` - Required. Unique identifier for the lock.
- `:divert?` - Optional. If true, automatically includes fencing tokens in all Datomic transactions while the lock is held (defaults to false).
- `:max-heartbeat-duration-ms` - Optional. How long before a lock is considered expired (defaults to 15ms).
- `:get-date-now` - Optional. Function that returns the current date (useful for testing).
- `:extra-data` - Optional. Additional data to store with the lock.
- `:env` - Optional. Lock environment (useful for testing).

## Development

### Testing

```bash
clojure -M:test
```

### Building

```bash
clojure -T:build jar
```

## License

Copyright © Kenny Williams

Distributed under the Eclipse Public License 1.0.
