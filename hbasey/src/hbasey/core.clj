; run command `lein run` to run
(ns hbasey.core
  (:import [java.util UUID]
           [org.apache.hadoop.hbase.util Bytes])
  (:require [clojure-hbase.core :as hb]
            [clojure-hbase.admin :as hba]))

(def tbl-name (str "clojure-hbase-test-db" (UUID/randomUUID)))

(defn -main [& args]
  (println (hba/master-running?))

  (map #(Bytes/toString (.getName %)) (hba/list-tables))

  (hba/create-table (hba/table-descriptor tbl-name))
  (hba/disable-table tbl-name)
  (hba/add-column-family tbl-name (hba/column-descriptor "mycf"))
  (hba/enable-table tbl-name)

  (println)
  (hb/with-table [the-tbl (hb/table tbl-name)]
    (hb/put the-tbl "testrow" :values [:mycf [:c1 "test" :c2 "test2"]])
    (println (hb/get the-tbl "testrow" :columns [:mycf [:c1 :c2]]))
    (println)
    (hb/with-scanner [scan-results (hb/scan the-tbl)]
      (println (map #(Bytes/toString (.getRow %1)) (seq scan-results))))

    (hb/delete the-tbl "testrow" :columns [:mycf [:c1 :c2]])
    (println (hb/get the-tbl "testrow" :columns [:mycf [:c1 :c2]]))))


(comment
  (def tab (hb/table "clojure-hbase-test-dbff197d50-91b3-41f3-b4af-e89ef2fa5322"))

(hb/put tab "datarow" :values [:mycf [:c1 "hello" :c2 "there"]])

(Bytes/toString (.value (hb/get tab "datarow" :columns [:mycf [:c1 :c2]])))

(hb/release-table tab)

(hba/disable-table tbl-name)
(hba/delete-table tbl-name)
)
