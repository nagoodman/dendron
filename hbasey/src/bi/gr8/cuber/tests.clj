(comment

  ;HBASE
  ; tests during dev

  (def blargo [[0 [0] 1 [0, 1] 2 [0, 1, 2] 3 [0, 3] 4 [0, 3, 4] 5 [5] 6 [5, 6] 7 [5, 6, 7] 8 [5, 8] 9 [5, 8, 9] ]
               [ 0 [0] 1 [0, 1] 2 [0, 1, 2] 3 [0, 3] 4 [0, 3, 4] 5 [5] 6 [5, 6] 7 [5, 6, 7] 8 [5, 8] 9 [5, 8, 9] ]])
  (def orig ^{:dimensions [:d2 :d2] :measure :val :N 10}
 {[0 0] 3 [0 1] 5 [0 2] 1 [0 3] 2 [0 4] 2 [0 5] 4 [0 6] 6 [0 7] 3 [0 8] 3 [0 9] 1 [1 0] 7 [1 1] 3 [1 2] 2 [1 3] 6 [1 4] 8 [1 5] 7 [1 6] 1 [1 7] 2 [1 8] 4 [1 9] 2 [2 0] 2 [2 1] 4 [2 2] 2 [2 3] 3 [2 4] 3 [2 5] 3 [2 6] 4 [2 7] 5 [2 8] 7 [2 9] 4 [3 0] 3 [3 1] 2 [3 2] 1 [3 3] 5 [3 4] 3 [3 5] 5 [3 6] 2 [3 7] 8 [3 8] 2 [3 9] 1 [4 0] 4 [4 1] 2 [4 2] 1 [4 3] 3 [4 4] 3 [4 5] 4 [4 6] 7 [4 7] 1 [4 8] 3 [4 9] 2 [5 0] 2 [5 1] 3 [5 2] 3 [5 3] 6 [5 4] 1 [5 5] 8 [5 6] 5 [5 7] 1 [5 8] 1 [5 9] 2 [6 0] 4 [6 1] 5 [6 2] 2 [6 3] 7 [6 4] 1 [6 5] 9 [6 6] 3 [6 7] 3 [6 8] 4 [6 9] 1 [7 0] 2 [7 1] 4 [7 2] 2 [7 3] 2 [7 4] 3 [7 5] 1 [7 6] 9 [7 7] 1 [7 8] 3 [7 9] 3 [8 0] 5 [8 1] 4 [8 2] 3 [8 3] 1 [8 4] 3 [8 5] 2 [8 6] 1 [8 7] 9 [8 8] 6 [8 9] 5 [9 0] 6 [9 1] 1 [9 2] 2 [9 3] 4 [9 4] 2 [9 5] 1 [9 6] 3 [9 7] 1 [9 8] 5 [9 9] 2})
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
      (doseq [[dim md] (map-indexed vector blargo)]
        (doseq [[idx [k v]] (map-indexed vector (partition 2 md))]
          ; for building
          (hb/put keytab k :value [d-k-hb-fam (str dim "-dimkey") idx])
          ;(println k "=>" idx)
          (hb/put keytab idx :value [d-k-hb-fam (str dim "-namekey") k])
          ;(println idx "=>" k "\n")
          )))

  (for [x (range 10) y (range 10)]
    (do (println "adding" [x y] (get orig [x y]))
    (add-row-to-cube tab keytab [0 0] [[x y] (get orig [x y])] 10 :sum)))

  (time (println (count (apply list (sum-cube-borders tab keytab [0 0] 10)))))

  (def a (for [day (range 10) st (range 10)]
           [[day st] (read-val tab [day st] :sum)]))

  (cube2csv a)

  ; note the three or so differing cells from the paper! we're right,
  ; paper is wrong.


  ;DYNAMODB
  (dyndb-debug)

  (def tab {:name "dyndb-debug-data-table" :cred creds})
  (def keytab {:name "dyndb-debug-key-table" :cred creds})

  ;(store-val tbl [0 0] 3 :sum)
  (time (let [keytab keytab]
          (doseq [[dim md] (map-indexed vector blargo)]
            (doseq [[idx [k v]] (map-indexed vector (partition 2 md))]
              ; for building
              (store-raw keytab (str dim "-dimkey-" idx) k)
              ;(println k "=>" idx)
              (store-raw keytab (str dim "-namekey-" k) idx)
              ;(println idx "=>" k "\n")
              ))))

  (time (for [x (range 10) y (range 10)]
    (do (println "adding" [x y] (get orig [x y]))
    (add-row-to-cube tab keytab [0 0] [[x y] (get orig [x y])] 10 :sum))))

  (time (println (apply list (sum-cube-borders tab keytab [0 0] 10))))

  (def a (for [day (range 10) st (range 10)]
           [[day st] (read-val tab [day st] :sum)]))

  (cube2csv a)

  )
