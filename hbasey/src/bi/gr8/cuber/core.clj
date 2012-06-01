(ns bi.gr8.cuber.core
  (:gen-class)
  (:require [bi.gr8.cuber.cube :as cube]
            [clojure.set])
  (:use [bi.gr8.cuber storage]
        [clojure.pprint]
        [clojure.repl]
        [clojure.java.io]))

(defn in? [hay need]
  (not= -1 (.indexOf hay need)))

(defn -main [& args]
  (println "Don't run this directly."))

(comment
  "This is for demo purposes."

(use 'bi.gr8.cuber.core)
(ns bi.gr8.cuber.core)

  (binding [*noisy?* true] (time (construct-cube "testwith22k" "22kdata.csv")))

  (query-cube "testwith22k" ["2010-08-10" "DL" "DCA"])

)

(def data-throughput {:read 100 :write 70})
(def keydata-throughput {:read 50 :write 5})

(defn construct-cube [name & csvs]
  (let [tbl (dyndb-table name)
        tbl-keys (dyndb-key-table name)]
    (try (create-table tbl :dyndb {:hash-key d-dyn-fam :throughput data-throughput})
      (create-table tbl-keys :dyndb {:hash-key d-k-dyn-fam :throughput keydata-throughput})
      (Thread/sleep 45000)
      (println "Tables created.")
      (catch Exception e (println "Tables already exist.")))
    (println "Creating keys...")
    (let [[origin N] [[0 0 0] 190]
          ;(cube/create-key-int-map csvs tbl-keys)
          ]
      (store-N tbl-keys N)
      (println "Prepped for cube with origin" origin "and size" N)
      (println "Inserting data...")
      (dorun (pmap (fn [csv] (cube/insert-row-by-row tbl tbl-keys origin csv N)) csvs))
      (println "Summing borders...")
      ;(println (dorun (apply list (cube/sum-cube-borders tbl tbl-keys origin N))))
      (println "Finished."))))

(defn query-cube [name namedcell]
  (let [tbl (dyndb-table name)
        tbl-keys (dyndb-key-table name)
        cell (if (:numeric (meta namedcell))
               namedcell
               (cube/names2nums tbl-keys namedcell))
        N (get-N tbl-keys)]
    (cube/query tbl tbl-keys cell N :sum)))

