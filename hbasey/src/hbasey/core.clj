; run command `lein run` to run
(ns hbasey.core
  (:import [java.util UUID]
           [org.apache.hadoop.hbase.util Bytes])
  (:require [clojure-hbase.core :as hb]
            [clojure-hbase.admin :as hba]
            [hbasey.dcubeutils :as dcu])
  (:use [hbasey.testdata]))
;(use :reload 'hbasey.newmodel)
(declare cube2d-meta keytab tab)

;(def tbl-name (str "clojure-hbase-test-db" (UUID/randomUUID)))

(def d-tbl "hbase-data-table")
(def d-fam "dfam")

(def d-md-tbl "hbase-metadata-table")
(def d-md-fam "dmdfam")

(def d-k-tbl "hbase-keymap-table")
(def d-k-fam "dkfam")

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
  (hba/enable-table d-md-tbl)
  ; keymap tbl
  (hba/create-table (hba/table-descriptor d-k-tbl))
  (hba/disable-table d-k-tbl)
  (hba/add-column-family d-k-tbl (hba/column-descriptor d-k-fam))
  (hba/enable-table d-k-tbl))

(defn clean-d-tbl []
  (hba/disable-table d-tbl)
  (hba/delete-table d-tbl)
  (hba/disable-table d-md-tbl)
  (hba/delete-table d-md-tbl)
  (hba/disable-table d-k-tbl)
  (hba/delete-table d-k-tbl)
  (create-d-tbl))

(defn find-methods [re class]
  (keep #(re-find re (str %1)) (.getMethods class)))

;(pprint (find-methods #".+to[A-Z].+" Bytes))
;(pprint (find-methods #"blah" (class (Bytes.))))
; = 3 (Bytes/toLong (byte-array 8 (map byte [0x0 0x0 0x0 0x0 0x0 0x0 0x0 2r11])))


(defn str-name "Reads out a byte-array name x from hbase as str" [x]
  (Bytes/toString (.getName x)))

(defn read-str-vals [tab k column]
  (let [value-vec (read-string (last (first
                    (hb/as-vector (hb/get tab k :column column)
                                  :map-family #(keyword (Bytes/toString %))
                                  :map-qualifier #(keyword (Bytes/toString %))
                                  :map-timestamp #(java.util.Date. %)
                                  :map-value #(Bytes/toString %)
                                  str))))]
    value-vec))

(defn read-long-vals [tab k column]
  (let [value-vec (last (first
                    (hb/as-vector (hb/get tab k :column column)
                                  :map-family #(keyword (Bytes/toString %))
                                  :map-qualifier #(keyword (Bytes/toString %))
                                  :map-timestamp #(java.util.Date. %)
                                  :map-value #(Bytes/toLong %)
                                  str)))]
    value-vec))

(defn read-sum-val [tab dimensions]
  (try
    (let [res (read-long-vals tab dimensions [d-fam :sum])]
      ;(if-let [real (:goto res)]
      ;  (read-sum-val tab real)
        res);)
    (catch NullPointerException e 0)))

(def really-store? false)

(defn store-sum-val [cube cell sum]
  (let [cell (vec cell)]
    ;(print "storing sum" cell "=>" sum "; ")
    (if really-store? ;(println "new val:"
             (.incrementColumnValue cube
                           (hb/to-bytes cell)
                           (hb/to-bytes d-fam)
                           (hb/to-bytes :sum)
                           sum))));)
    ;(hb/put cube cell :value [d-fam :sum sum])))


(defn read-md-val [tab dimension k]
  (read-str-vals tab k [d-md-fam (str dimension "-anchor")]))

(defn read-name2key [tab dimension name]
  (read-long-vals tab name [d-k-fam (str dimension "-dimkey")]))
(def read-name2key (memoize read-name2key))

(defn read-key2name [tab dimension k]
  (try
    (read-str-vals tab k [d-k-fam (str dimension "-namekey")])
    (catch NullPointerException e nil)))
(def read-key2name (memoize read-key2name))

(defrecord Cube [table]
  clojure.lang.IFn
  (invoke [this] this)
  (invoke [this cell] (read-sum-val table cell)) 
  (applyTo [this args] (clojure.lang.AFn/applyToHelper this args)))

(defn app [args]
  (clean-d-tbl)

  (comment (hb/with-table [tab (hb/table d-tbl)]
    (doseq [[k v] cube2d]
      (hb/put tab k :value [d-fam :measures [:sum v]]))))

  (time
  (let [mdtab (hb/table d-md-tbl)
        keytab (hb/table d-k-tbl)]
    (doseq [[dim md] (map-indexed vector cube2d-meta)]
      (doseq [[idx [k v]] (map-indexed vector (partition 2 md))]
        ; for querying
        (hb/put mdtab idx :value [d-md-fam (str dim "-anchor") v])
        ;(println idx "=>" v)
        ; for building
        (hb/put keytab k :value [d-k-fam (str dim "-dimkey") idx])
        ;(println k "=>" idx)
        (hb/put keytab idx :value [d-k-fam (str dim "-namekey") k])
        ;(println idx "=>" k "\n")
        ))
    (hb/release-table mdtab)
    (hb/release-table keytab)))

  (time (do
          (add-row-to-cube tab [0 0] [[[2010 :q1] :ca] 2])
          (add-row-to-cube tab [0 0] [[[2010 :q1] :wa] 2]) ; (0,2)
          (add-row-to-cube tab [0 0] [[[2010 :q2] :ca] 5]) ; (1,0)
          (add-row-to-cube tab [0 0] [[[2010 :q2] :or] 4]) ; (1,1)
          (add-row-to-cube tab [0 0] [[[2010 :q2] :wa] 3]) ; (1,2)
          (add-row-to-cube tab [0 0] [[[2010 :q3] :ca] 2])
          (add-row-to-cube tab [0 0] [[[2010 :q4] :or] 2]) ; (3,1)
          (add-row-to-cube tab [0 0] [[[2011 :q1] :wa] 4]) ; (4,2)
          (add-row-to-cube tab [0 0] [[[2011 :q2] :ca] 2]) ; (5,0)
          ))
  )

(comment

(def tab (hb/table d-tbl))
(def mdtab (hb/table d-md-tbl))
(def keytab (hb/table d-k-tbl))

(read-sum-val tab [[2010 :q2] :or])
(read-sum-val tab [[2011 :q1] :ca])

;(read-md-val mdtab [2010 :q2])

(read-md-val mdtab 0 1)

(read-key2name keytab 0 1) ; [2010 :q2]

(read-name2key keytab 0 [2010 :q2]) ; 1

(read-name2key keytab 0 [2010 :q1])


(time (query-cube tab mdtab [[2010 :q3] :wa])) ; expecting 18

(loop [cube table datas sparse2d]
  (if (seq datas)
    (recur (make-cube-v1 cube (first datas) cube2d-meta) (next datas))
   cube)) ; should output cube2d

);;;;

(defn matching-borders [cell anchor]
  (map (fn [[i v]] (assoc anchor i v)) (map-indexed vector cell)))
(def matching-borders (memoize matching-borders))

(defn query-cube [cube cubemd cell]
  (let [dimwise-level-anchors (map
                                (fn [[idx val]]
                                  (read-md-val cubemd idx val))
                                (map-indexed vector cell))
        dims (count dimwise-level-anchors)
        anchors (partition dims (apply interleave dimwise-level-anchors))]
    (reduce +
            (map
              ; this and the next map can be replaced with pmap but seems slower
              (fn [anchor]
                (let [bords (matching-borders cell (vec anchor))]
                  (reduce + (map #(read-sum-val cube %1)
                                  (conj bords anchor)))))
              anchors))))

(defn names2nums [namedcell]
  (map (fn [[dim name]] (read-name2key keytab dim name))
    (map-indexed vector namedcell)))
(defn names2nums (memoize names2nums))

(defmacro pand "parallel and" [& forms]
  (let [r# (eval `(pvalues ~@forms))]
    `(and ~@r#)))

(defn get-dependent-anchors [origin cell N]
  ; includes cell itself if it's an anchor
  (if (= N 1) (list cell)
    (let [anchors (dcu/anchor-slots origin N)]
      (filter (fn [anchor] (every? (fn [[ai ci]] (>= ai ci)) (partition 2 (interleave anchor cell)))) anchors))))

; [0,2] => [5,2]
(defn get-opposing-border [anchor cell N]
  (map #(let [sum (+ %2 (/ N 2))]
          (if (and (= %1 %2)
                   (<= sum N))
            sum
            %2))
    anchor cell))
(def get-opposing-border (memoize get-opposing-border))

(comment (defn get-opposing-border [anchor cell N]
  (map (fn [ai ci]
            (let [sum (+ ci (/ N 2))]
              (if (<= sum N)
                (if (= ai ci) sum
                  ci)
                ci)))
    anchor cell))

(get-opposing-border [3 1] [4 1] 4)
(get-opposing-border [3 1] [3 2] 4)
)

(defn cell-in-keys? [cell]
  (every? (fn [[dim k]] (read-key2name keytab dim k))
    (map-indexed vector cell)))
(def cell-in-keys? (memoize cell-in-keys?))

; called for each row
(defn add-row-to-cube 
  ([cube origin row] (add-row-to-cube cube origin row 10))
  ([cube origin row N]
  (let [[namedcell data] row
        cell (names2nums namedcell)
        location (dcu/where-is-cell cell origin N)]
    (cond
      (= location :anchor)
        (let [anchors-to-update (get-dependent-anchors origin cell N)]
          (doseq [anchor anchors-to-update]
            (if (cell-in-keys? anchor)
              (store-sum-val cube anchor data))))
      (= location :border)
        (let [anchors-to-update (get-dependent-anchors origin cell N)
              border-to-update (get-opposing-border origin cell N)]
          (doseq [to-update (conj anchors-to-update border-to-update cell)]
            (if (cell-in-keys? to-update)
              (store-sum-val cube to-update data))))
      :else
        (let [anchors-to-update (get-dependent-anchors origin cell N)
              anchor (map #(int (- %1 %2)) location (repeat (/ N 4)))
              intersecting-borders (dcu/intersecting-borders cell anchor)
              borders-to-update (filter identity
                                        (map #(let [ret (get-opposing-border
                                                          anchor %1 N)]
                                                (if (= ret %1) nil ret))
                                             intersecting-borders))]
          (doseq [to-update (concat anchors-to-update borders-to-update)]
            (if (cell-in-keys? to-update)
              (store-sum-val cube to-update data)))
          (add-row-to-cube cube (map + anchor (repeat 1)) row (dec (/ N 2))))
      ))))

(comment

(def really-store? (not really-store?))
really-store?

)



;called at the end of the initial cube-creation process
(defn sum-cube-borders [cube]
  )

(comment

(defn make-cube-v1-graaargh-don't-use [cube [[cell data]] cubemd]
  ; version 1, we assume for the sake of brevity that the metadata has
  ; already been defined for us.
  ; (In practice, this means the keys are known up front as well as
  ; the total max-N of the data in any dimension.)
  (let [dimwise-level-anchors (map #(read-md-val cubemd %1) cell)
        dims (count dimwise-level-anchors)
        anchors (partition dims (apply interleave dimwise-level-anchors))
        current-data (read-sum-val cube cell)
        _ (println "a: " anchors " cur: " current-data)
        ]
    ; just update the relevant anchors and intersecting border cells at all
    ; levels; do the border sums later.
    (doseq [[idx anchor] (map-indexed vector anchors)]
      (println idx anchor)
      (println (nth (first cube2d-meta-part2) idx))
      )
    (comment
      (store-sum-val cube cell (+ current-data data))
      )
  ))

(defn get-dependent-anchors [anchor]
  ; gets the anchors that are "more right" or "more down" than the given anchor
  (map (fn [[dim_idx dim_val]]
         ; find dimension, get its pair
         (some (fn [levels]
                 (some (fn [level] (if (= anchor (first (:anchor-pairs level))) (:anchor-pairs level) nil)) levels)
                 ) (nth cube2d-meta-part2 dim_idx))
         )
    (map-indexed vector anchor)
  ))

);;;;

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

