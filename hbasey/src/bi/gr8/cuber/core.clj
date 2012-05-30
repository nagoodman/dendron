(ns bi.gr8.cuber.core
  (:import [java.util UUID]
           [org.apache.hadoop.hbase.util Bytes])
  (:require [clojure-hbase.core :as hb]
            [bi.gr8.cuber.cube :as cube]
            [clojure.set])
  (:use [bi.gr8.cuber storage]
        [clojure.pprint]
        [clojure.java.io]))

(defn in? [hay need]
  (not= -1 (.indexOf hay need)))

(defn test-data-load []
  (hbase-debug)

  (println "mapping keys to ints...")
  ;41,1,134
  (let [tab (hb/table "hbase-debug-data-table")
        keytab (hb/table "hbase-debug-keymap-table")]

    (cube/create-key-int-map "22kdata.csv" keytab)
    (println "inserting...")
    (println (cube/insert-row-by-row tab keytab [0 0 0] "22kdata.csv" 190))
    (println "summing...")
    ;(binding [*print-right-margin* 100] (pprint (get-all-border-regions [0 0] 10)))
    ;(pprint (get-all-border-regions [0 0 0] 190))
    (println "using those bords")
    (println (apply list (cube/sum-cube-borders tab keytab [0 0 0] 190)))



  ))

(defn test-query []
  (comment

  (def tab (hb/table "hbase-debug-data-table"))
  (def keytab (hb/table "hbase-debug-keymap-table"))
  (cube/query tab (map-indexed #(read-key2name keytab %1 %2) [40 0 94]) keytab 190 :sum)

  (def a (for [day (range 41) st (range 134)]
           [[day st] (read-val tab [day 0 st] :sum)]))

    )
  (let [tab (hb/table "hbase-debug-data-table")
        keytab (hb/table "hbase-debug-keymap-table")]
    (cube/query tab (map-indexed #(read-key2name keytab %1 %2) [40 0 133]) keytab 190 :sum)
  )
  )

(defn -main [& args]
  (println '(clojure.core/use 'clojure.core))
  (clojure.main/repl))
 ; (cond
 ;   (in? args "load") (test-data-load)
 ;   (in? args "query") (test-query)))
    ;:else (do
    ;        (clojure.main/repl))))
            ;(clojure.core/use 'clojure.core))))

(defn construct-cube [name & csvs]
  (let [tbl (dyndb-table name)
        tbl-keys (dyndb-key-table name)]
    (println "Creating keys...")
    (let [[origin N] (cube/create-key-int-map csvs tbl-keys)]
      (println "Inserting data...")
      (doseq [csv csvs]
        (cube/insert-row-by-row tbl tbl-keys origin csv N))
      (println "Summing borders...")
      (println (apply list (cube/sum-cube-borders tbl tbl-keys origin N))))))

(defn query-cube [name namedcell]
  (let [tbl (dyndb-table name)
        tbl-keys (dyndb-key-table name)
        cell (if (:numeric (meta namedcell))
               namedcell
               (cube/names2nums tbl-keys namedcell))
        N (get-N tbl-keys)]
    (cube/query tbl tbl-keys cell N :sum)))

