(ns bi.gr8.cuber.storage
  "Methods for talking to the key-value store hosting the data."
  (:import [org.apache.hadoop.hbase.client HTablePool$PooledHTable]
           [org.apache.hadoop.hbase.util Bytes]
           [com.amazonaws.services.dynamodb.model
                AttributeValue
                AttributeAction
                AttributeValueUpdate
                UpdateItemRequest
                Key
                ReturnValue])
  (:require [clojure-hbase.core :as hb]
            [clojure-hbase.admin :as hba]
            [rotary.client :as dyndb]))

(load-file "credentials.clj")

(defmacro defmethod-mem
  "Creates and installs a new memoized method of multimethod associated with dispatch-value. "
  [multifn dispatch-val & fn-tail]
  `(. ~(with-meta multifn {:tag 'clojure.lang.MultiFn}) addMethod ~dispatch-val (memoize (fn ~@fn-tail))))

(def d-hb-fam "dfam")
(def d-k-hb-fam "dkfam")

(def d-dyn-fam {:name "hash_id" :type "S"})
(def d-k-dyn-fam d-dyn-fam) ; same table

(def ^:dynamic *noisy?* false)
(def ^:dynamic *really-store?* true)

(declare read-val hbase-read-str-vals hbase-read-long-vals)

; Create methods

(defmulti create-table (fn [tbl kind & rest] kind))

(defmethod create-table :hbase [tbl _ fam]
  (hba/create-table (hba/table-descriptor tbl))
  (hba/disable-table tbl)
  (hba/add-column-family tbl (hba/column-descriptor fam))
  (hba/enable-table tbl))

(defmethod create-table :dyndb [tbl _ {:keys [hash-key range-key throughput] :as options}]
  (dyndb/create-table (:cred tbl) (assoc options :name (:name tbl))))

(defmulti clean-table (fn [tbl] (class tbl)))

(defmethod clean-table java.lang.String [tbl] ; hbase
  (hba/disable-table tbl)
  (hba/delete-table tbl))

(defmethod clean-table clojure.lang.PersistentArrayMap [tbl] ;dyndb
  (dyndb/delete-table (:cred tbl) (:name tbl)))

(defn hbase-debug []
  (let [tbl "hbase-debug-data-table"
        ktbl "hbase-debug-keymap-table"]
    (try 
    (clean-table tbl)
    (clean-table ktbl)
      (catch Exception e nil))
    (create-table tbl :hbase d-hb-fam)
    (create-table ktbl :hbase d-k-hb-fam)))

(defn dyndb-table [name]
  {:name name :cred {:access-key accessKey :secret-key secretKey}})
(def dyndb-tbl dyndb-table) ; alias

(defn dyndb-key-table [name]
  (dyndb-table name));(str name "-keymd")))

(defn get-origin-N [keytab]
  (read-string (get (dyndb/get-item (:cred keytab) (:name keytab) "cube-origin-N") "value")))

(defn store-origin-N [keytab origin N]
  (dyndb/put-item (:cred keytab) (:name keytab) {(:name d-k-dyn-fam) "cube-origin-N" "value" (str [origin N])}))

(defn get-N [keytab]
  (second (get-origin-N keytab)))

(defn dyndb-debug [tbl ktbl]

  (try (clean-table tbl)
    (catch Exception e nil))
  (try (clean-table ktbl)
    (catch Exception e nil))
  (Thread/sleep 30000)

  ; pricing is $0.01/hr for every 50 reads, 10 writes. First 10 and 5 are free.
  (create-table tbl :dyndb
    {:hash-key d-dyn-fam :throughput {:read 50 :write 40}}) ; (1+4)cents/hr=$1.20/day

  (create-table ktbl :dyndb
    {:hash-key d-k-dyn-fam :throughput {:read 50 :write 5}}) ; 1cent/hr=24cents/day

  ; 100 MB storage free, $1/GB-month after.

  (dyndb/describe-table (:cred tbl) (:name tbl))
  )

; Store methods

(defmulti store-val (fn [tab cell val kind] [(class tab) kind]))

(defmethod store-val [HTablePool$PooledHTable :sum] [tab cell val kind]
  (let [cell (vec cell)]
    (if *really-store?*
      (let [res (.incrementColumnValue tab
                             (hb/to-bytes cell)
                             (hb/to-bytes d-hb-fam)
                             (hb/to-bytes :sum)
                             val)]
        (if *noisy?* (println "inc'd" cell "by" val "to" res))
        res)
      (let [orig (read-val tab cell :sum)]
        (print "would have inc'd" cell "by" val "to go from" orig "to ")
        (println (+ orig val) "; ")
        (+ orig val)))))

(defmethod store-val [clojure.lang.PersistentArrayMap :sum] [tab cell val kind]
  (let [items {"value" (doto (AttributeValueUpdate.)
                      (.withAction AttributeAction/ADD)
                      (.withValue (.withN (AttributeValue.) (str val))))}
        k (Key. (AttributeValue. (str (vec cell))))
        up-result (.updateItem (dyndb/db-client (:cred tab))
                 (doto (UpdateItemRequest.)
                   (.setTableName (:name tab))
                   (.setKey k)
                   (.setReturnValues ReturnValue/UPDATED_NEW)
                   (.setAttributeUpdates items)))]
    (Long/parseLong (.getN (get (.getAttributes up-result) "value")))))

; NOTE: using this is ~15% slower in a test.
(defn store-val-async [agent cube anchor data kind]
  (store-val cube anchor data kind))

(defmulti store-raw (fn [tab key value] (class tab)))

(defmethod store-raw HTablePool$PooledHTable [tab key [col value]]
  (hb/put tab (str key) :value [d-k-hb-fam col (str value)]))

(defmethod store-raw clojure.lang.PersistentArrayMap [tab key value]
  (try (dyndb/put-item (:cred tab) (:name tab) {(:name d-k-dyn-fam) (str key) "value" (str value)})
    (catch Exception e ; went over provisioned throughput or something else
      (println "Exception" (str e) "on" key value)
      (let [sleep-time (* 1000 (inc (rand 20)))]
        (println "Retrying in" sleep-time)
        (Thread/sleep sleep-time)
        (println "Retrying" key value)
        (store-raw tab key value)))))

; Read methods

(defmulti read-name2key
  "Given the integer of a dimension key, return its named equivalent."
  (fn [tab dim nm] (class tab)))

(defmethod-mem read-name2key HTablePool$PooledHTable [tab dim nm]
  (try
    (hbase-read-str-vals tab (str nm) [d-k-hb-fam (str dim "-namekey")])
  (catch NullPointerException e ; doesn't exist
    nil)))

(defmethod-mem read-name2key :default [tab dim nm]
  ;(println dim nm)
  (try
    (Long/parseLong (get (dyndb/get-item (:cred tab) (:name tab) (str dim "-namekey-" nm)) "value"))
  (catch java.lang.NumberFormatException e
    (println "Problem reading name2key" (str dim "-namekey-" nm))
    (println "Please verify that key exists and report a bug if it does.")
    (throw e))))

(defmulti read-key2name
  "Given the name of a dimension key, return its integer key equivalent."
  (fn [tab dim k] (class tab)))

(defmethod-mem read-key2name HTablePool$PooledHTable [tab dim k]
  (hbase-read-str-vals tab (str k) [d-k-hb-fam (str dim "-dimkey")]))

(defmethod-mem read-key2name :default [tab dim k]
  (let [res (str (get (dyndb/get-item (:cred tab) (:name tab) (str dim "-dimkey-" k)) "value"))]
    (if (empty? res) nil res)))

(defmulti read-val
  "0 means either not found or actually 0. To be sure if it doesn't exist,
  use `read-key2name` or the handy `cell-in-keys?`"
  (fn [tab dimensions kind] [(class tab) kind]))

(defmethod read-val [HTablePool$PooledHTable :sum] [tab dimensions kind]
  ;(if *noisy?*
  ;  (hb/with-table [keytab (hb/table "hbase-debug-keymap-table")]
  ;    (print "reading" dimensions (map-indexed #(read-key2name keytab %1 %2)
  ;                                             dimensions))))
  (try
    (let [res (hbase-read-long-vals tab dimensions [d-hb-fam :sum])]
  ;    (if *noisy?* (println " as " res))
      (or res 0))
  (catch NullPointerException e
    0)))


(defn hbase-read-str-vals [tab k column]
  (let [value-vec (last (first
                    (hb/as-vector (hb/get tab k :column column)
                                  :map-family #(keyword (Bytes/toString %))
                                  :map-qualifier #(keyword (Bytes/toString %))
                                  :map-timestamp #(java.util.Date. %)
                                  :map-value #(Bytes/toString %)
                                  str)))]
    (try (read-string value-vec)
      (catch NumberFormatException e value-vec))))

(defn hbase-read-long-vals [tab k column]
  (let [value-vec (last (first
                    (hb/as-vector (hb/get tab k :column column)
                                  :map-family #(keyword (Bytes/toString %))
                                  :map-qualifier #(keyword (Bytes/toString %))
                                  :map-timestamp #(java.util.Date. %)
                                  :map-value #(Bytes/toLong %)
                                  str)))]
    value-vec))

(defmethod read-val :default [tab dimensions kind]
  (let [res (Long/parseLong (or (get (dyndb/get-item (:cred tab) (:name tab) (str (vec dimensions))) "value") "0"))]
    (if *noisy?* (println "read" dimensions "got" res))
    res))

