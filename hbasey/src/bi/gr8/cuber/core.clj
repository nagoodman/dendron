(ns bi.gr8.cuber.core
  (:import [java.util UUID]
           [org.apache.hadoop.hbase.util Bytes])
  (:require [clojure-hbase.core :as hb]
            [clojure-hbase.admin :as hba]
            [rotary.client :as dyndb]
            [clojure.set])
  (:use [bi.gr8.cuber cube storage]
        [clojure.pprint]
        [clojure.java.io]))

(defn in? [hay need]
  (not= -1 (.indexOf hay need)))

(defn test-data-load []
  ;(hbase-debug)

  (comment
  (println "mapping keys to ints...")
  ;41,1,134
  (create-key-int-map "22kdata.csv")
  (println "inserting...")
  (println (insert-row-by-row tab [0 0 0] "22kdata.csv" 190))
  (println "summing...")
  (binding [*print-right-margin* 100] (pprint (get-all-border-regions [0 0] 10)))
  ;(pprint (get-all-border-regions [0 0 0] 190))
  (println "using those bords")
  (println (count (apply list (sum-cube-borders tab keytab [0 0 0] 190))))


  (query-cube tab keytab (map-indexed #(read-key2name keytab %1 %2) [1 0 1]) 190)

  ;(hb/with-table [tab (hb/table d-tbl)]
  ;  (let [_ (map #(str (ffirst %1) "," (nth (first %1) 2) "," (second %1) "\n")
  ;               (filter #(not (zero? (second %1)))
  ;                       (for [day (range 41) st (range 134)]
  ;                         [[day 0 st] (read-sum-val tab [day 0 st])])))]
  ;    ["stored " (count _) " of total " (* 41 133)]))

  ;27,0,4
  (println "querying")
  (println (map-indexed #(read-name2key keytab %1 %2) ["2011-03-17" "DL" "ATL"]))

  (time (println (query-cube tab ["2010-01-14" "DL" "ORF"] 190)))

  (println (map-indexed #(read-key2name keytab %1 %2) [2 0 2]))
  (println (query-cube tab ["2010-02-13" "DL" "ALB"] 190))
)

  )

(defn test-query []

  )

(defn -main [& args]
  (cond
    (in? args "load") (test-data-load)
    (in? args "query") (test-query)))
