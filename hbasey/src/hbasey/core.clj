; run command `lein run` to run
(ns hbasey.core
  (:import [java.util UUID]
           [org.apache.hadoop.hbase.util Bytes])
  (:require [clojure-hbase.core :as hb]
            [clojure-hbase.admin :as hba]
            [hbasey.dcubeutils :as dcu])
  (:use [hbasey.testdata]
        [hbasey.newmodel]))

(declare keytab tab add-row-to-cube sum-cube-borders query-cube clean-d-tbl)

(def d-tbl "hbase-data-table")
(def d-fam "dfam")

(def d-md-tbl "hbase-metadata-table")
(def d-md-fam "dmdfam")

(def d-k-tbl "hbase-keymap-table")
(def d-k-fam "dkfam")

(declare app)
(defn -main [& args]
  (if (hba/master-running?) (do (clean-d-tbl) (time (app args)))))


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
        (or res 0));)
    (catch NullPointerException e 0)))

(def really-store? true)

(defn store-sum-val [cube cell sum]
  (let [cell (vec cell)]
    (if really-store? 
      (.incrementColumnValue cube
                             (hb/to-bytes cell)
                             (hb/to-bytes d-fam)
                             (hb/to-bytes :sum)
                             sum)
      (let [orig (read-sum-val cube cell)]
        (print "would have inc'd" cell "by" sum "to go from" orig "to ")
        (print (+ orig sum) "; ")
        (+ orig sum)))))


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
  (def tab (hb/table d-tbl))
  (def mdtab (hb/table d-md-tbl))
  (def keytab (hb/table d-k-tbl))


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

  (time (println (apply list (sum-cube-borders tab [0 0] 10))))

  (hb/with-table [tab (hb/table d-tbl)]
    (println (for [day (range 6) st (range 3)]
              [[day st] (read-sum-val tab [day st])])))


  (time (println (query-cube tab mdtab [[2010 :q3] :wa]))) ; expecting 18

  )

(comment

(read-sum-val tab [[2010 :q2] :or])
(read-sum-val tab [[2011 :q1] :ca])

(read-sum-val tab [1 1])

;(read-md-val mdtab [2010 :q2])

(read-md-val mdtab 0 1)

(read-key2name keytab 0 1) ; [2010 :q2]

(read-name2key keytab 0 [2010 :q2]) ; 1

(read-name2key keytab 0 [2010 :q1])



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
                                  (read-md-val cubemd idx (read-name2key keytab idx val)))
                                (map-indexed vector cell))
        dims (count dimwise-level-anchors)
        anchors (partition dims (apply interleave dimwise-level-anchors))]
    (reduce +
            (map
              ; this and the next map can be replaced with pmap but seems slower
              (fn [anchor]
                (let [bords (matching-borders cell (vec anchor))]
                  (reduce + (map #(read-sum-val cube (vec (map-indexed (fn [dim n] (read-name2key keytab dim n)) %1)))
                                  (conj bords anchor)))))
              anchors))))

(defn names2nums [namedcell]
  (map (fn [[dim name]] (read-name2key keytab dim name))
    (map-indexed vector namedcell)))
(def names2nums (memoize names2nums))

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

(defn change-at-key
  "Given a non-vec list,
  does the equivalent of (assoc (vec lst) k (f (nth (vec lst) k)))"
  [lst k f]
  (map-indexed #(if (= %1 k)
                  (f %2)
                  %2)
    lst))


(defn get-all-border-regions [origin N]
  ; return a seq of all pairs of border regions in a cube
  ; (with the start offset by 1 so that we can start by reading,
  ; increment, increment, ...)
  (if (> N 4)
    (let [anchors (map vec (dcu/anchor-slots origin N))
          dims (count origin)
          ; each anchor specifies the start of dims ranges
          ranges (map (fn [anchor]
                        (loop [dim 0 pairs []]
                          (if (< dim dims)
                            (recur (inc dim)
                                   (conj pairs
                                         [(assoc anchor dim (inc (nth anchor dim)))
                                          (assoc anchor dim (+ (nth anchor dim) (dec (/ N 2))))]
                                         ))
                            pairs)))
                      anchors)
          sub-N (dec (/ N 2))
          sub-ranges (map (fn [anchor]
                            (let [sub-origin (map inc anchor)]
                              (get-all-border-regions sub-origin sub-N)))
                          anchors)]
      (if (every? identity sub-ranges) (conj sub-ranges ranges) ranges))
    nil))

;(pprint (get-all-border-regions [0 0] 10))

;called at the end of the initial cube-creation process
(defn sum-cube-borders [cube origin N]
  (let [dim (count origin)
        bord-regs (get-all-border-regions origin N)]
    (map (fn [box]
           (map-indexed (fn [idx [start end]]
                          (if (cell-in-keys? start)
                            (loop [cell start, value (read-sum-val cube start)]
                              (if (and (< (nth cell idx) (nth end idx))
                                       (or value (not= cell start)))
                                (let [next-cell (assoc cell idx (inc (nth cell idx)))]
                                  (if (cell-in-keys? next-cell)
                                    (recur next-cell
                                           (store-sum-val cube next-cell value))))))))
                        box))
         bord-regs)
    ))

