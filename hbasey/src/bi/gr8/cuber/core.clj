(ns bi.gr8.cuber.core
  (:gen-class)
  (:require [bi.gr8.cuber.cube :as cube]
            [clojure.set])
  (:use [bi.gr8.cuber storage]
        [clojure.pprint]
        [clojure.repl]
        [clojure.java.io]))

(defn -main [& args]
  (println "Don't run this directly."))

(comment
  "This is for demo purposes."

(do (use 'bi.gr8.cuber.core) (ns bi.gr8.cuber.core))

(binding [*noisy?* true] (time (construct-cube "testwith22k" "22kdata.csv" "22kdata.csv")))

(defn f [x] (str "/dev/shm/" x))
(time (apply construct-cube "testbig" (map f '(On_Time_On_Time_Performance_2001_3.csv On_Time_On_Time_Performance_2001_4.csv))))

(query-cube "testwith22k" ["2010-08-10" "DL" "DCA"])

)

(def data-throughput {:read 100 :write 1000})
(def construct-keys true)

(defn construct-cube [name & csvs]
  (let [tbl (dyndb-table name)
        tbl-keys (dyndb-key-table name)]
    (try (create-table tbl :dyndb {:hash-key d-dyn-fam :throughput data-throughput})
      (println "Waiting 45 secs for table creation...")
      (Thread/sleep 45000)
      (println "Table created.")
      (catch Exception e (println "Tables already exist.")))
    (if construct-keys (println "Creating keys..."))
    (let [[origin N counts] (if construct-keys
                              (cube/create-key-int-map csvs tbl-keys)
                              (get-origin-N tbl-keys))]
      (println "Prepped for cube with origin" origin "and size" N)
      (println "Inserting data...")
      (dorun (map #(cube/insert-row-by-row tbl tbl-keys origin %1 N) csvs))
      (println "Summing borders...")
      (cube/sum-borders tbl tbl-keys origin N counts)
      (println "Finished."))))

(defn query-cube [name namedcell]
  (let [tbl (dyndb-table name)
        tbl-keys (dyndb-key-table name)
        cell (if (:numeric (meta namedcell))
               namedcell
               (cube/names2nums tbl-keys namedcell))
        N (get-N tbl-keys)]
    (cube/query tbl tbl-keys cell N :sum)))

