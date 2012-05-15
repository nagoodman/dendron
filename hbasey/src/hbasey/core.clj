; run command `lein run` to run
(ns hbasey.core
  (:import [java.util UUID]
           [org.apache.hadoop.hbase.util Bytes])
  (:require [clojure-hbase.core :as hb]
            [clojure-hbase.admin :as hba]
            [hbasey.dcubeutils :as dcu])
  (:use [hbasey.testdata]))

;(def tbl-name (str "clojure-hbase-test-db" (UUID/randomUUID)))

(def d-tbl "hbase-data-table")
(def d-fam "dfam")

(def d-md-tbl "hbase-metadata-table")
(def d-md-fam "dmdfam")

(declare app)
(defn -main [& args]
  (if (hba/master-running?) (app args)))


(defn create-d-tbl []
  (hba/create-table (hba/table-descriptor d-tbl))
  (hba/disable-table d-tbl)
  (hba/add-column-family d-tbl (hba/column-descriptor d-fam))
  (hba/enable-table d-tbl)
  ; meta tbl
  (hba/create-table (hba/table-descriptor d-md-tbl))
  (hba/disable-table d-md-tbl)
  (hba/add-column-family d-md-tbl (hba/column-descriptor d-md-fam))
  (hba/enable-table d-md-tbl))

(defn clean-d-tbl []
  (hba/disable-table d-tbl)
  (hba/delete-table d-tbl)
  (hba/disable-table d-md-tbl)
  (hba/delete-table d-md-tbl)
  (create-d-tbl))


(defn str-name "Reads out a byte-array name x from hbase as str" [x]
  (Bytes/toString (.getName x)))

(defn read-vals [tab k column]
  (let [value-vec (read-string (last (first
                    (hb/as-vector (hb/get tab k
                                          :column column)
                                  :map-family #(keyword (Bytes/toString %))
                                  :map-qualifier #(keyword (Bytes/toString %))
                                  :map-timestamp #(java.util.Date. %)
                                  :map-value #(Bytes/toString %) str))))
        ]
    value-vec))

(defn read-sum-val [tab dimensions]
  (try
    (let [res ((apply hash-map (read-vals tab (vec dimensions) [d-fam :measures])) :sum)]
      (if-let [real (:goto res)]
        (read-sum-val tab real)
        res))
    (catch NullPointerException e 0)))

(defn read-md-val [tab k]
  (read-vals tab k [d-md-fam :anchor]))

(defrecord Cube [table]
  clojure.lang.IFn
  (invoke [this] this)
  (invoke [this cell] (read-sum-val table cell)) 
  (applyTo [this args] (clojure.lang.AFn/applyToHelper this args)))

(comment

(def tab (hb/table d-tbl))
(def mdtab (hb/table d-md-tbl))

(read-sum-val tab [[2010 :q2] :or])
(read-sum-val tab [[2011 :q1] :ca])

(read-md-val mdtab [2010 :q2])

(time (query-cube tab mdtab [[2010 :q3] :wa])) ; expecting 18

(loop [cube table datas sparse2d]
  (if (seq datas)
    (recur (make-cube-v1 cube (first datas) cube2d-meta) (next datas))
   cube)) ; should output cube2d

);;;;

(defn matching-borders [cell anchor]
  (map (fn [[i v]] (assoc anchor i v)) (map-indexed vector cell)))

(defn query-cube [cube cubemd cell]
  (let [dimwise-level-anchors (map #(read-md-val cubemd %1) cell)
        dims (count dimwise-level-anchors)
        anchors (partition dims (apply interleave dimwise-level-anchors))
        ]
    (reduce +
            (map
              ; this and the next map can be replaced with pmap but seems slower
              (fn [anchor]
                (let [bords (matching-borders cell (vec anchor))]
                  (reduce + (map #(read-sum-val cube %1)
                                  (conj bords anchor)))))
              anchors))))

(defn store-sum-val [cube cell sum]
  (hb/put cube cell :value [d-fam :measures [:sum sum]]))

(defn make-cube-v1 [cube [[cell data]] cubemd]
  ; version 1, we assume for the sake of brevity that the metadata has
  ; already been defined for us.
  ; (In practice, this means the keys are known up front as well as
  ; the total max-N of the data in any dimension.)
  ; basically this is just the cube update function assuming
  ; everything initially starts with 0
  (let [dimwise-level-anchors (map #(read-md-val cubemd %1) cell)
        dims (count dimwise-level-anchors)
        anchors (partition dims (apply interleave dimwise-level-anchors))
        current-data (read-sum-val cube cell)
        _ (println "a: " anchors " cur: " current-data)
        ]
    (doseq [[idx anchor] (map-indexed vector anchors)]
      (println idx anchor)
      (println (nth (first cube2d-meta-part2) idx))
      )
    (comment
      (store-sum-val cube cell (+ current-data data))
      )
  ))

(make-cube-v1 tab (vec {[[2010 :q1] :ca] 2}) mdtab)

(make-cube-v1 tab (vec {[[2010 :q1] :wa] 2}) mdtab)
(make-cube-v1 tab (vec {[[2010 :q2] :ca] 5}) mdtab)

(make-cube-v1 tab (vec {[[2010 :q2] :or] 4}) mdtab)

(make-cube-v1 tab (vec {[[2010 :q2] :wa] 3}) mdtab)

(make-cube-v1 tab (vec {[[2010 :q3] :ca] 2}) mdtab)
(make-cube-v1 tab (vec {[[2010 :q4] :or] 2}) mdtab)
(make-cube-v1 tab (vec {[[2011 :q1] :wa] 4}) mdtab)
(make-cube-v1 tab (vec {[[2011 :q2] :ca] 2}) mdtab)

(defn get-dependent-anchors [anchor]
  )

(defn app [args]
  (clean-d-tbl)

  (hb/with-table [tab (hb/table d-tbl)]
    (doseq [[k v] cube2d]
      (hb/put tab k :value [d-fam :measures [:sum v]])))

  (hb/with-table [tab (hb/table d-md-tbl)]
    (doseq [[k v] cube2d-meta]
      (hb/put tab k :value [d-md-fam :anchor v])))

  )

(defn get-cardinality [tab]
  ; 1, 4, 10, 22, 46...
  10)

(defn oldapp [args]
  (if (not-any? #(= d-tbl %1) (map str-name (hba/list-tables)))
    (create-d-tbl))

  (if (not= -1 (.indexOf args "test")) ; insert test-data
    (do (clean-d-tbl)
      (let [tab (hb/table d-tbl)] ; example of direct table ctrl (manual release)
        ;(dcu/cube2csv expected)
        (doseq [[k v] expected]
          (hb/put tab k :value [d-fam :measures [:sum v]]))

        (hb/release-table tab))))

  (if (not= -1 (.indexOf args "readtest"))
    (hb/with-table [tab (hb/table d-tbl)]
      (doseq [[k v] expected]
        (print (= v (read-sum-val tab k))))))

  ;(def tcbue (Cube. ttab))
  ;(query-ddc-range (with-meta tcube {:N 10}) [7 8])

  (if (not= -1 (.indexOf args "query"))
    (hb/with-table [tab (hb/table d-tbl)] ; read out a query, say to 7,8
      ; or to Q4,OR (for Alaska-subset)
      (let [cube (with-meta (Cube. tab) {:op + :N (get-cardinality tab)})]
        (dcu/query-ddc-range cube [0 0] [7 8])
      )))
  
  (if (not= -1 (.indexOf args "singlecell"))
    (hb/with-table [tab (hb/table d-tbl)]
      (let [cube (with-meta (Cube. tab) {:op + :N (get-cardinality tab)})]
        (+ (- (dcu/query-ddc-range cube [0 0] [7 8])
              (dcu/query-ddc-range cube [0 0] [6 8])
              (dcu/query-ddc-range cube [0 0] [7 7]))
           (dcu/query-ddc-range cube [0 0] [6 7])))))

  (if (not= -1 (.indexOf args "test2"))
    (do (clean-d-tbl)
      (hb/with-table [tab (hb/table d-tbl)]
        (doseq [[k v] sparse-cube2d]
          (hb/put tab k :value [d-fam :measures [:sum v]])))))

  (if (not= -1 (.indexOf args "readtest2"))
    (hb/with-table [tab (hb/table d-tbl)]
      (doseq [[k v] sparse-cube2d]
        (print (= v (read-sum-val tab k))))))

  (if (not= -1 (.indexOf args "query2"))
    (hb/with-table [tab (hb/table d-tbl)]
      (let [cube (with-meta (Cube. tab) {:op + :N 22})]
        (dcu/query-ddc-range cube [ 0] [7 8])
      )))

  )

