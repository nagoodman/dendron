(comment

  ;HBASE
  ; tests during dev
  ; with blargo and orig we have our 2D "if this doesn't work all is lost"
  ; sanity check.
  ; with blargo3 and a 10x10x10 cube of counts we also do some check queries
  ; to make sure the border summing works.

  (require '[clojure.math.combinatorics :as combin])
  (require '[clojure-hbase.core :as hb])
  (import '[org.apache.hadoop.hbase.util Bytes])
  (def blargo [[0 [0] 1 [0, 1] 2 [0, 1, 2] 3 [0, 3] 4 [0, 3, 4] 5 [5] 6 [5, 6] 7 [5, 6, 7] 8 [5, 8] 9 [5, 8, 9] ]
               [ 0 [0] 1 [0, 1] 2 [0, 1, 2] 3 [0, 3] 4 [0, 3, 4] 5 [5] 6 [5, 6] 7 [5, 6, 7] 8 [5, 8] 9 [5, 8, 9] ]])
  (def orig ^{:dimensions [:d2 :d2] :measure :val :N 10}
 {[0 0] 3 [0 1] 5 [0 2] 1 [0 3] 2 [0 4] 2 [0 5] 4 [0 6] 6 [0 7] 3 [0 8] 3 [0 9] 1 [1 0] 7 [1 1] 3 [1 2] 2 [1 3] 6 [1 4] 8 [1 5] 7 [1 6] 1 [1 7] 2 [1 8] 4 [1 9] 2 [2 0] 2 [2 1] 4 [2 2] 2 [2 3] 3 [2 4] 3 [2 5] 3 [2 6] 4 [2 7] 5 [2 8] 7 [2 9] 4 [3 0] 3 [3 1] 2 [3 2] 1 [3 3] 5 [3 4] 3 [3 5] 5 [3 6] 2 [3 7] 8 [3 8] 2 [3 9] 1 [4 0] 4 [4 1] 2 [4 2] 1 [4 3] 3 [4 4] 3 [4 5] 4 [4 6] 7 [4 7] 1 [4 8] 3 [4 9] 2 [5 0] 2 [5 1] 3 [5 2] 3 [5 3] 6 [5 4] 1 [5 5] 8 [5 6] 5 [5 7] 1 [5 8] 1 [5 9] 2 [6 0] 4 [6 1] 5 [6 2] 2 [6 3] 7 [6 4] 1 [6 5] 9 [6 6] 3 [6 7] 3 [6 8] 4 [6 9] 1 [7 0] 2 [7 1] 4 [7 2] 2 [7 3] 2 [7 4] 3 [7 5] 1 [7 6] 9 [7 7] 1 [7 8] 3 [7 9] 3 [8 0] 5 [8 1] 4 [8 2] 3 [8 3] 1 [8 4] 3 [8 5] 2 [8 6] 1 [8 7] 9 [8 8] 6 [8 9] 5 [9 0] 6 [9 1] 1 [9 2] 2 [9 3] 4 [9 4] 2 [9 5] 1 [9 6] 3 [9 7] 1 [9 8] 5 [9 9] 2})
  (def blargo3 (take 3 (repeat [0 [0] 1 [0, 1] 2 [0, 1, 2] 3 [0, 3] 4 [0, 3, 4] 5 [5] 6 [5, 6] 7 [5, 6, 7] 8 [5, 8] 9 [5, 8, 9] ])))
  (defn cube2csv [cube] ;2d only of course
    (let [sorted-cube (sort cube) ;(sort-cube cube)
          [[N _] _] (last sorted-cube)]
      (loop [rows (partition (inc N) sorted-cube)]
        (when-first [row rows]
          (println
            (apply str (drop-last (apply str (map #(str (second %1) ",") row)))))
          (recur (next rows))))))

  (hbase-debug)

  (def tab (hb/table "hbase-debug-data-table"))
  (def keytab (hb/table "hbase-debug-keymap-table"))

  (let [keytab keytab]
    (doseq [[dim md] (map-indexed vector blargo3)]
      (doseq [[idx [k v]] (map-indexed vector (partition 2 md))]
        ; for building
        (hb/put keytab (str k) :value [d-k-hb-fam (str dim "-dimkey") (str idx)])
        (hb/put keytab (str idx) :value [d-k-hb-fam (str dim "-namekey") (str k)])
        )))

  (defn mapp [result]
    (hb/latest-as-map result :map-family #(keyword (Bytes/toString %))
      :map-qualifier #(keyword (Bytes/toString %))
      :map-value #(Bytes/toString %)))

  (hb/with-scanner [res (hb/scan keytab)]
    (pprint (doall (map mapp (-> res .iterator iterator-seq)))))

  (binding [*noisy?* true]
    (for [x (range 10) y (range 10)]
      (do (println "adding" [x y] (get orig [x y]))
        (cube/add-row-to-cube tab keytab [0 0] [[x y] (get orig [x y])] 10 :sum)))
    )

  (binding [*noisy?* true]
    (dorun (for [x (range 10) y (range 10) z (range 10)]
      (do (println "adding" [x y z]);(get orig [x y]))
        (cube/add-row-to-cube tab keytab [0 0 0] [[x y z] 1] 10 :sum))))
    )

   (binding [*noisy?* true]
    (dorun (for [x (range 4) y (range 4) z (range 4)]
      (do (println "adding" [x y z]);(get orig [x y]))
        (cube/add-row-to-cube tab keytab [0 0 0] [[x y z] 1] 4 :sum)))))

(binding [*noisy?* true *really-store?* true] (cube/sum-borders tab keytab [0 0 0] 4))

(binding [*noisy?* true *really-store?* true] (cube/sum-borders tab keytab [0 0 0] 10))

(binding [*noisy?* true *really-store?* true] (cube/sum-borders tab keytab [0 0] 10))

  (def a (for [x (range 4) y (range 4) z (range 4)]
           [[x y z] (read-val tab [x y z] :sum)]))

  (def a (for [day (range 10) st (range 10)]
           [[day st] (read-val tab [day st] :sum)]))

  (cube2csv a)

(assert (= a '([[0 0] 3] [[0 1] 5] [[0 2] 6] [[0 3] 8] [[0 4] 10] [[0 5] 17] [[0 6] 6] [[0 7] 9] [[0 8] 12] [[0 9] 13] [[1 0] 7] [[1 1] 3] [[1 2] 2] [[1 3] 11] [[1 4] 8] [[1 5] 33] [[1 6] 1] [[1 7] 2] [[1 8] 7] [[1 9] 2] [[2 0] 9] [[2 1] 4] [[2 2] 2] [[2 3] 9] [[2 4] 3] [[2 5] 50] [[2 6] 4] [[2 7] 5] [[2 8] 16] [[2 9] 4] [[3 0] 12] [[3 1] 9] [[3 2] 5] [[3 3] 28] [[3 4] 14] [[3 5] 69] [[3 6] 7] [[3 7] 15] [[3 8] 35] [[3 9] 7] [[4 0] 16] [[4 1] 2] [[4 2] 1] [[4 3] 6] [[4 4] 3] [[4 5] 86] [[4 6] 7] [[4 7] 1] [[4 8] 11] [[4 9] 2] [[5 0] 21] [[5 1] 19] [[5 2] 29] [[5 3] 54] [[5 4] 74] [[5 5] 126] [[5 6] 25] [[5 7] 45] [[5 8] 65] [[5 9] 77] [[6 0] 4] [[6 1] 5] [[6 2] 2] [[6 3] 14] [[6 4] 1] [[6 5] 28] [[6 6] 3] [[6 7] 3] [[6 8] 10] [[6 9] 1] [[7 0] 6] [[7 1] 4] [[7 2] 2] [[7 3] 8] [[7 4] 3] [[7 5] 42] [[7 6] 9] [[7 7] 1] [[7 8] 13] [[7 9] 3] [[8 0] 11] [[8 1] 13] [[8 2] 7] [[8 3] 30] [[8 4] 7] [[8 5] 60] [[8 6] 13] [[8 7] 13] [[8 8] 39] [[8 9] 9] [[9 0] 17] [[9 1] 1] [[9 2] 2] [[9 3] 7] [[9 4] 2] [[9 5] 76] [[9 6] 3] [[9 7] 1] [[9 8] 9] [[9 9] 2])))

  ; should be 27
; should read 15 vals:
;        1        2        2        4        2        4        4
; l0: [0,0,0], [0,2,0], [0,0,2], [0,2,2]; [2,0,0], [2,2,0], [2,0,2]
;        1        1        1        1        1        1        1
; l1: [1,1,1], [1,2,1], [1,1,2], [1,2,2]; [2,1,1], [2,2,1], [2,1,2]
;        1
; l2: [2,2,2]
  (binding [*noisy?* true] (cube/query tab keytab [2 2 2] 10 :sum))

; [2 2 1] should be 18
;        1        2        1        2        2        4        2
; l0: [0 0 0], [0 2 0], [0 0 1], [0 2 1]; [2 0 0], [2 2 0], [2 0 1]
;        1        1        1        1
; l1: [1 1 1], [1 2 1], [2 1 1], [2 2 1]. done!
(binding [*noisy?* true] (cube/query tab keytab [2 2 1] 10 :sum))

(binding [*noisy?* true] (cube/query tab keytab [2 2 3] 10 :sum))

(1 2 3) as  1
(2 2 3) as  2
(1 1 3) as  3
(2 1 3) as  1
5

(binding [*noisy?* true] (cube/query tab keytab [9 9 9] 10 :sum))

(binding [*noisy?* true *really-store?* false] (cube/add-row-to-cube tab keytab [0 0 0] [[2 1 1] 1] 10 :sum))

(binding [*noisy?* true *really-store?* false] (cube/add-row-to-cube tab keytab [0 0 0] [[1 2 2] 1] 10 :sum))


(binding [*noisy?* true] (cube/query tab keytab [3 3 3] 4 :sum))
need +9

reading [2 3 2] (2 3 2) as  7
reading [3 3 2] (3 3 2) as  2
reading [2 3 3] (2 3 3) as  2
reading [2 2 3] (2 2 3) as  7
reading [3 2 2] (3 2 2) as  7
reading [3 2 3] (3 2 3) as  2

  (binding [*noisy?* true] (cube/query tab keytab [2 2 0] 4 :sum))
  (reduce + (for [x (range (inc 2)) y (range (inc 2)) z (range (inc 0))] 1))

(doseq [c (combin/selections (range 4) 3)]
  (println c
           (cube/query tab keytab c 4 :sum)
           (reduce + (for [x (range (inc (first c)))
                           y (range (inc (second c)))
                           z (range (inc (nth c 2)))] 1))))

  (cube/query tab keytab [7 8 3] 10 :sum)

  (reduce + (for [x (range 8) y (range 9) z (range 4)] 1))

  (cube/query tab keytab [7 8 0] 10 :sum)

  (reduce + (for [x (range 6) y (range 6) z (range 1)] 1))

  (read-val tab [2 1 2] :sum)

  (query-cube tab [9 9] keytab 10 :sum)

  ; note the three or so differing cells from the paper! we're right,
  ; paper is wrong.


  ;DYNAMODB


(def dtab (dyndb-table "dyndb-debug-data-table"))
(def dkeytab (dyndb-table "dyndb-debug-key-table"))

(dyndb-debug dtab dkeytab)

  ;(store-val tbl [0 0] 3 :sum)
(time (let [keytab dkeytab]
        (doseq [[dim md] (map-indexed vector blargo3)]
          (doseq [[idx [k v]] (map-indexed vector (partition 2 md))]
            ; for building
            (store-raw keytab (str dim "-dimkey-" idx) k)
            ;(println k "=>" idx)
            (store-raw keytab (str dim "-namekey-" k) idx)
            ;(println idx "=>" k "\n")
            ))))

  (binding [*noisy?* true]
    (for [x (range 10) y (range 10) z (range 10)]
      (do (println "adding" [x y z]);(get orig [x y]))
        (cube/add-row-to-cube dtab dkeytab [0 0 0] [[x y z] 1] 10 :sum))))

  (time (for [x (range 10) y (range 10)]
    (do (println "adding" [x y] (get orig [x y]))
    (add-row-to-cube tab keytab [0 0] [[x y] (get orig [x y])] 10 :sum))))

  (apply list (cube/sum-cube-borders dtab dkeytab [0 0 0] 10))


(def b (for [x (range 10) y (range 10) z (range 10)]
         [[x y z] (read-val dtab [x y z] :sum)]))

(pprint b)

  (binding [*noisy?* true]
    (cube/query dtab dkeytab [9 9 9] 10 :sum))

  (binding [*noisy?* true] (cube/query dtab dkeytab [2 2 2] 10 :sum)) 

  (binding [*noisy?* true]
    (cube/query dtab dkeytab [5 5 5] 10 :sum)) ;216

  (def a (for [day (range 10) st (range 10)]
           [[day st] (read-val tab [day st] :sum)]))

  (cube2csv a)

  )

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

