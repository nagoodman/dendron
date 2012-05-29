; run command `lein run` to run
(ns hbasey.core
  (:import [java.util UUID]
           [org.apache.hadoop.hbase.util Bytes])
  (:require [clojure-hbase.core :as hb]
            [clojure-hbase.admin :as hba]
            [hbasey.dcubeutils :as dcu]
            [clojure.set])
  (:use [hbasey.testdata]
        [hbasey.newmodel]
        [clojure.pprint]
        [clojure.java.io]))

(declare keytab tab add-row-to-cube sum-cube-borders query-cube clean-d-tbl)

(def d-tbl "hbase-data-table")
(def d-fam "dfam")

(def d-k-tbl "hbase-keymap-table")
(def d-k-fam "dkfam")

(declare app app-other)
(defn -main [& args]
  (println args)
  (if (and false (hba/master-running?))
    (do
      (clean-d-tbl)
      (time (app-other args))
      (time (app args)))))


(defn create-d-tbl []
  (hba/create-table (hba/table-descriptor d-tbl))
  (hba/disable-table d-tbl)
  (hba/add-column-family d-tbl (hba/column-descriptor d-fam))
  (hba/enable-table d-tbl)
  ; keymap tbl
  (hba/create-table (hba/table-descriptor d-k-tbl))
  (hba/disable-table d-k-tbl)
  (hba/add-column-family d-k-tbl (hba/column-descriptor d-k-fam))
  (hba/enable-table d-k-tbl))

(defn clean-d-tbl []
  (hba/disable-table d-tbl)
  (hba/delete-table d-tbl)
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
  (let [value-vec (last (first
                    (hb/as-vector (hb/get tab k :column column)
                                  :map-family #(keyword (Bytes/toString %))
                                  :map-qualifier #(keyword (Bytes/toString %))
                                  :map-timestamp #(java.util.Date. %)
                                  :map-value #(Bytes/toString %)
                                  str)))]
    (try (read-string value-vec)
      (catch NumberFormatException e value-vec))))

(defn read-long-vals [tab k column]
  (let [value-vec (last (first
                    (hb/as-vector (hb/get tab k :column column)
                                  :map-family #(keyword (Bytes/toString %))
                                  :map-qualifier #(keyword (Bytes/toString %))
                                  :map-timestamp #(java.util.Date. %)
                                  :map-value #(Bytes/toLong %)
                                  str)))]
    value-vec))

(defn read-key2name [tab dimension k]
  (try
    (read-str-vals tab k [d-k-fam (str dimension "-namekey")])
    (catch NullPointerException e nil)))
(def read-key2name (memoize read-key2name))

(defn read-sum-val
  "0 means either not found or actually 0. To be sure if it doesn't exist,
  use (cell-in-keys?)"
  [tab dimensions]
  (print "reading" dimensions (map-indexed #(read-key2name keytab %1 %2) dimensions))
  (try
    (let [res (read-long-vals tab dimensions [d-fam :sum])]
      ;(if-let [real (:goto res)]
      ;  (read-sum-val tab real)
      (println " as " res)
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


(defn read-name2key [tab dimension name]
  (read-long-vals tab name [d-k-fam (str dimension "-dimkey")]))
(def read-name2key (memoize read-name2key))

(defn matching-borders [cell anchor]
  (map (fn [[i v]] (assoc anchor i v)) (map-indexed vector cell)))
(def matching-borders (memoize matching-borders))

(defn log2 [x]
  (long (Math/floor (/ (Math/log x) (Math/log 2)))))

(defn relative-anchors
  "Return array of relative anchors, the 0th entry is the root-most
  anchor, and so on, until it reaches itself."
  ([N numkey] (relative-anchors 0 N numkey))
  ([left N numkey]
    (let [half (+ left (/ N 2))
          next-n (dec (/ N 2))]
      (if (and (<= left numkey)
               (>= N 1))
        (if (>= numkey half)
          (conj (relative-anchors (inc half) next-n numkey) half)
          (conj (relative-anchors (inc left) next-n numkey) left))
        nil))))


(defn names2nums [namedcell]
  (map (fn [[dim name]] (read-name2key keytab dim name))
    (map-indexed vector namedcell)))
(def names2nums (memoize names2nums))

(defn query-cube [cube namedcell N]
  (let [cell (names2nums namedcell)
        dimwise-level-anchors (map #(relative-anchors N %1) cell)
        dims (count dimwise-level-anchors)
        anchors (partition dims (apply interleave dimwise-level-anchors))]
    (reduce +
            ; this and the next map can be replaced with pmap but seems slower
            (map
              (fn [anch]
                (let [anchor (vec anch)
                      bords (matching-borders cell anchor)]
                  (reduce + (map #(read-sum-val cube %1) (conj bords anchor)))))
              anchors))))

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

;called at the end of the initial cube-creation process
(defn sum-cube-borders [cube origin N]
  (let [dim (count origin)
        bord-regs (get-all-border-regions origin N)]
    (map (fn [box]
           (map-indexed (fn [idx [start end]]
                          (if (cell-in-keys? start)
                            (loop [cell start, value (read-sum-val cube start)]
                              (println "inloop," end idx cell value)
                              (if (and (< (nth cell idx)
                                          (nth end idx))
                                       (or value (not= cell start)))
                                (let [next-cell (assoc cell idx (inc (nth cell idx)))]
                                  (if (cell-in-keys? next-cell)
                                    (do (println "storing" next-cell "=" value)(recur next-cell
                                           (store-sum-val cube next-cell value)))))))))
                        box))
         bord-regs)
    ))

(defn calculate-N [limit]
  (loop [N 4]
    (if (< N limit)
      (recur (* 2 (inc N)))
      N)))

(defn create-key-int-map [file]
  (let [data (slurp file)
        lines (.split data "\n")
        dims (count (first lines))
        keyset (loop [lines (.split data "\n") md-keyset []]
                 (if (seq lines)
                   (let [line (seq (.split (first lines) ","))]
                     (recur (next lines)
                            (vec (map-indexed #(clojure.set/union (get md-keyset %1) (sorted-set %2)) line))))
                   md-keyset))
        counts (map count keyset)
        N (calculate-N (apply max counts))]
    (let [keytab (hb/table d-k-tbl)]
      (doseq [[dim keys] (map-indexed vector keyset)]
        (doseq [[idx name] (map-indexed vector keys)]
          (hb/put keytab name :value [d-k-fam (str dim "-dimkey") idx])
          (hb/put keytab idx :value [d-k-fam (str dim "-namekey") name])
          ))
      (hb/release-table keytab))
    (println counts N)))

(defn insert-row-by-row [cube origin datafile N]
  (with-open [rdr (reader datafile)]
    (doseq [line (line-seq rdr)]
      (let [cell (seq (.split line ","))
            value 1] ; should come from somewhere...
        (add-row-to-cube cube origin [cell 1] N)))))

(defn app [args]
  (def tab (hb/table d-tbl))
  (def keytab (hb/table d-k-tbl))

  (println "mapping keys to ints...")
  ;41,1,134
  (create-key-int-map "src/hbasey/22kdata.csv")
  (println "inserting...")
  (println (insert-row-by-row tab [0 0 0] "src/hbasey/22kdata.csv" 190))
  (println "summing...")
  (binding [*print-right-margin* 100] (pprint (get-all-border-regions [0 0] 10)))
  ;(pprint (get-all-border-regions [0 0 0] 190))
  (println "using those bords")
  (println (apply list (sum-cube-borders tab [0 0 0] 190)))


  (query-cube tab (map-indexed #(read-key2name keytab %1 %2) [1 0 1]) 190)

  ;(hb/with-table [tab (hb/table d-tbl)]
  ;  (let [_ (map #(str (ffirst %1) "," (nth (first %1) 2) "," (second %1) "\n")
  ;               (filter #(not (zero? (second %1)))
  ;                       (for [day (range 41) st (range 134)]
  ;                         [[day 0 st] (read-sum-val tab [day 0 st])])))]
  ;    ["stored " (count _) " of total " (* 41 133)]))

  ;27,0,4
  (println "querying")
  (println (map-indexed #(read-name2key keytab %1 %2) ["2011-03-17" "DL" "ATL"]))
  (time (println (query-cube tab ["2011-03-17" "DL" "ATL"] 190)))

  (println (map-indexed #(read-key2name keytab %1 %2) [40 0 133]))
  (time (println (query-cube tab ["2011-12-16" "DL" "VPS"] 190)))

  (time (println (query-cube tab ["2011-09-16" "DL" "DCA"] 190)))

  (println (map-indexed #(read-key2name keytab %1 %2) [0 0 95]))
  (time (println (query-cube tab ["2010-01-14" "DL" "ORF"] 190)))

  (println (map-indexed #(read-key2name keytab %1 %2) [2 0 2]))
  (println (query-cube tab ["2010-02-13" "DL" "ALB"] 190))

  )
  
(defn app-other [args]
  (def tab (hb/table d-tbl))
  (def keytab (hb/table d-k-tbl))

  (time
    (let [keytab (hb/table d-k-tbl)]
      (doseq [[dim md] (map-indexed vector cube2d-meta)]
        (doseq [[idx [k v]] (map-indexed vector (partition 2 md))]
          ; for building
          (hb/put keytab k :value [d-k-fam (str dim "-dimkey") idx])
          ;(println k "=>" idx)
          (hb/put keytab idx :value [d-k-fam (str dim "-namekey") k])
          ;(println idx "=>" k "\n")
          ))
      (hb/release-table keytab)))

  (time (do
          (add-row-to-cube tab [0 0] [[[2010 :q1] :ca] 2] 10)
          (add-row-to-cube tab [0 0] [[[2010 :q1] :wa] 2] 10) ; (0,2)
          (add-row-to-cube tab [0 0] [[[2010 :q2] :ca] 5] 10) ; (1,0)
          (add-row-to-cube tab [0 0] [[[2010 :q2] :or] 4] 10) ; (1,1)
          (add-row-to-cube tab [0 0] [[[2010 :q2] :wa] 3] 10) ; (1,2)
          (add-row-to-cube tab [0 0] [[[2010 :q3] :ca] 2] 10)
          (add-row-to-cube tab [0 0] [[[2010 :q4] :or] 2] 10) ; (3,1)
          (add-row-to-cube tab [0 0] [[[2011 :q1] :wa] 4] 10) ; (4,2)
          (add-row-to-cube tab [0 0] [[[2011 :q2] :ca] 2] 10) ; (5,0)
          ))

  (println "summing borders-o")
  (pprint (get-all-border-regions [0 0] 10))
  (println "using those bords")
  (time (println (apply list (sum-cube-borders tab [0 0] 10))))

  ;(hb/with-table [tab (hb/table d-tbl)]
  ;  (println (for [day (range 6) st (range 3)]
  ;            [[day st] (read-sum-val tab [day st])])))


  (println "query time")
  (time (println (query-cube tab [[2010 :q3] :wa] 10))) ; expecting 18
  ; 13 means the result of sum-cube-borders didn't run

  (println "relative anchors sanity check")
  (println (relative-anchors 10 0))
  (println (relative-anchors 10 1))
  (println (relative-anchors 10 2))
  (println (relative-anchors 10 3))
  (println (relative-anchors 10 4))
  (println (relative-anchors 10 5))
  (println (relative-anchors 10 6))
  (println (relative-anchors 10 7))
  (println (relative-anchors 10 8))
  (println (relative-anchors 10 9))
  ;(relative-anchors 10 2) ;N=10, 2 => 0 1 2
  ;(relative-anchors 10 6) ; 5, 6
  ;(relative-anchors 22 8) ; 0,6,7,8
  ;(relative-anchors 22 20) ; 11,17,20

  )

