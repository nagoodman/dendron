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

(def d-hb-fam "dfam")
(def d-k-hb-fam "dkfam")
(def d-dyn-fam {:name "d-sum" :type "S"})
(def d-k-dyn-fam {:name "d-k-id" :type "S"})

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

(defmulti clean-table (fn [tbl kind] kind))

(defmethod clean-table :hbase [tbl _]
  (hba/disable-table tbl)
  (hba/delete-table tbl))

(defmethod clean-table :dyndb [tbl _]
  (dyndb/delete-table (:cred tbl) (:name tbl)))

(defn hbase-debug []
  (let [tbl "hbase-debug-data-table"
        ktbl "hbase-debug-keymap-table"]
    (try 
    (clean-table tbl :hbase)
    (clean-table ktbl :hbase)
      (catch Exception e nil))
    (create-table tbl :hbase d-hb-fam)
    (create-table ktbl :hbase d-k-hb-fam)))


(defn dyndb-debug []

  (def secretKey "")
  (def accessKey "")
  (def creds {:access-key accessKey :secret-key secretKey})
  (def tbl {:name "dyndb-debug-data-table" :cred creds})
  (def ktbl {:name "dyndb-debug-key-table" :cred creds})

  (try (clean-table tbl :dyndb)
    (catch Exception e nil))
  (try (clean-table ktbl :dyndb)
    (catch Exception e nil))

  (create-table tbl :dyndb
    {:hash-key d-dyn-fam :throughput {:read 10 :write 5}})

  (create-table ktbl :dyndb
    {:hash-key d-k-dyn-fam :throughput {:read 10 :write 5}})


  (dyndb/describe-table (:cred tbl) (:name tbl))

  ;(store-val tbl [0 0] 3 :sum)


  )

; Store methods

(def ^:dynamic *really-store?* true)

(defmulti store-val (fn [tab cell val kind] [(class tab) kind]))

(defmethod store-val [HTablePool$PooledHTable :sum] [tab cell val kind]
  (let [cell (vec cell)]
    (if *really-store?*
      (.incrementColumnValue tab
                             (hb/to-bytes cell)
                             (hb/to-bytes d-hb-fam)
                             (hb/to-bytes :sum)
                             val)
      (let [orig (read-val tab cell :sum)]
        (print "would have inc'd" cell "by" val "to go from" orig "to ")
        (print (+ orig val) "; ")
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
    (Integer/parseInt (.getN (get (.getAttributes up-result) "value")))))

(defmulti store-raw (fn [tab key value] (class tab)))

(defmethod store-raw HTablePool$PooledHTable [tab key [col value]]
  (hb/put tab key :value [d-k-hb-fam col value]))

(defmethod store-raw clojure.lang.PersistentArrayMap [tab key value]
  (dyndb/put-item (:cred tab) (:name tab) {"d-k-id" (str key) "value" (str value)}))

; Read methods

(def ^:dynamic *noisy?* false)

(defmulti read-key2name
  "Given the integer of a dimension key, return its named equivalent."
  (fn [tab dim k] (class tab)))

(defmethod read-key2name HTablePool$PooledHTable [tab dim k]
  (try
    (hbase-read-str-vals tab k [d-k-hb-fam (str dim "-namekey")])
  (catch NullPointerException e ; doesn't exist
    nil)))

(defmethod read-key2name :default [tab dim k]
  (get (dyndb/get-item (:cred tab) (:name tab) (str dim "-namekey-" k)) "value"))

(defmulti read-name2key
  "Given the name of a dimension key, return its integer key equivalent."
  (fn [tab dim nm] (class tab)))

(defmethod read-name2key HTablePool$PooledHTable [tab dim nm]
  (hbase-read-long-vals tab nm [d-k-hb-fam (str dim "-dimkey")]))

(defmethod read-name2key :default [tab dim nm]
  (Integer/parseInt (get (dyndb/get-item (:cred tab) (:name tab) (str dim "-dimkey-" nm)) "value")))

(defmulti read-val
  "0 means either not found or actually 0. To be sure if it doesn't exist,
  use `read-key2name` or the handy `cell-in-keys?`"
  (fn [tab dimensions kind] [(class tab) kind]))

(defmethod read-val [HTablePool$PooledHTable :sum] [tab dimensions kind]
  (if *noisy?*
    (hb/with-table [keytab (hb/table "hbase-debug-keymap-table")]
      (print "reading" dimensions (map-indexed #(read-key2name keytab %1 %2)
                                               dimensions))))
  (try
    (let [res (hbase-read-long-vals tab dimensions [d-hb-fam :sum])]
      (if *noisy?* (println " as " res))
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
  (Integer/parseInt (or (get (dyndb/get-item (:cred tab) (:name tab) (str (vec dimensions))) "value") "0")))

