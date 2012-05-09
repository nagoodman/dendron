
; for testing purposes only
; some of this might make it into the final project!
; it's scattered and seemingly inconsistent because
; it's exploratory!

(ns hbasey.testddc
  (:import [java.util UUID]
           [org.apache.hadoop.hbase.util Bytes])
  (:require [clojure-hbase.core :as hb]
            [clojure-hbase.admin :as hba]
            [clojure.math.combinatorics :as combin]))

(comment "for posting to repl"

  (require '[clojure.math.combinatorics :as combin])

)

(def orig ^{:dimensions [:d1 :d2] :measure :val :N 10}
 [[0 0] 3 [0 1] 5 [0 2] 1 [0 3] 2 [0 4] 2 [0 5] 4 [0 6] 6 [0 7] 3 [0 8] 3 [0 9] 1 [1 0] 7 [1 1] 3 [1 2] 2 [1 3] 6 [1 4] 8 [1 5] 7 [1 6] 1 [1 7] 2 [1 8] 4 [1 9] 2 [2 0] 2 [2 1] 4 [2 2] 2 [2 3] 3 [2 4] 3 [2 5] 3 [2 6] 4 [2 7] 5 [2 8] 7 [2 9] 4 [3 0] 3 [3 1] 2 [3 2] 1 [3 3] 5 [3 4] 3 [3 5] 5 [3 6] 2 [3 7] 8 [3 8] 2 [3 9] 1 [4 0] 4 [4 1] 2 [4 2] 1 [4 3] 3 [4 4] 3 [4 5] 4 [4 6] 7 [0 0] 3 [0 1] 5 [0 2] 1 [0 3] 2 [0 4] 2 [0 5] 4 [0 6] 6 [0 7] 3 [0 8] 3 [0 9] 1 [1 0] 7 [1 1] 3 [1 2] 2 [1 3] 6 [1 4] 8 [1 5] 7 [1 6] 1 [1 7] 2 [1 8] 4 [1 9] 2 [2 0] 2 [2 1] 4 [2 2] 2 [2 3] 3 [2 4] 3 [2 5] 3 [2 6] 4 [2 7] 5 [2 8] 7 [2 9] 4 [3 0] 3 [3 1] 2 [3 2] 1 [3 3] 5 [3 4] 3 [3 5] 5 [3 6] 2 [3 7] 8 [3 8] 2 [3 9] 1 [4 0] 4 [4 1] 2 [4 2] 1 [4 3] 3 [4 4] 3 [4 5] 4 [4 6] 7 [4 7] 1 [4 8] 3 [4 9] 2 [5 0] 2 [5 1] 3 [5 2] 3 [5 3] 6 [5 4] 1 [5 5] 8 [5 6] 5 [5 7] 1 [5 8] 1 [5 9] 2 [6 0] 4 [6 1] 5 [6 2] 2 [6 3] 7 [6 4] 1 [6 5] 9 [6 6] 3 [6 7] 3 [6 8] 4 [6 9] 1 [7 0] 2 [7 1] 4 [7 2] 2 [7 3] 2 [7 4] 3 [7 5] 1 [7 6] 9 [7 7] 1 [7 8] 3 [7 9] 3 [8 0] 5 [8 1] 4 [8 2] 3 [8 3] 1 [8 4] 3 [8 5] 2 [8 6] 1 [8 7] 9 [8 8] 6 [8 9] 5 [9 0] 6 [9 1] 1 [9 2] 2 [9 3] 4 [9 4] 2 [9 5] 1 [9 6] 3 [9 7] 1 [9 8] 5 [9 9] 2])

(def expected  ^{:dimensions [:d1 :d2] :measure :val :N 10}
  [[0 0] 3 [0 1] 5 [0 2] 6 [0 3] 8 [0 4] 10 [0 5] 17 [0 6] 6 [0 7] 9
   [0 8] 12 [0 9] 13 [1 0] 7 [1 1] 3 [1 2] 2 [1 3] 11 [1 4] 8 [1 5] 33
   [1 6] 1 [1 7] 2 [1 8] 7 [1 9] 2 [2 0] 9 [2 1] 4 [2 2] 2 [2 3] 9 [2 4] 3
   [2 5] 50 [2 6] 4 [2 7] 5 [2 8] 7 [2 9] 4 [3 0] 12 [3 1] 9 [3 2] 5 [3 3] 28
   [3 4] 14 [3 5] 69 [3 6] 7 [3 7] 15 [3 8] 35 [3 9] 7 [4 0] 16 [4 1] 2
   [4 2] 1 [4 3] 6 [4 4] 3 [4 5] 86 [4 6] 7 [4 7] 1 [4 8] 11 [4 9] 2
   [5 0] 21 [5 1] 19 [5 2] 29 [5 3] 54 [5 4] 74 [5 5] 126 [5 6] 25 [5 7] 45
   [5 8] 65 [5 9] 77 [6 0] 4 [6 1] 5 [6 2] 2 [6 3] 14 [6 4] 1 [6 5] 28
   [6 6] 3 [6 7] 3 [6 8] 10 [6 9] 1 [7 0] 6 [7 1] 4 [7 2] 2 [7 3] 8 [7 4] 3
   [7 5] 42 [7 6] 9 [7 7] 1 [7 8] 13 [7 9] 3 [8 0] 11 [8 1] 13 [8 2] 7
   [8 3] 30 [8 4] 7 [8 5] 60 [8 6] 13 [8 7] 9 [8 8] 39 [8 9] 9 [9 0] 17
   [9 1] 1 [9 2] 2 [9 3] 7 [9 4] 2 [9 5] 76 [9 6] 3 [9 7] 1 [9 8] 9 [9 9] 2])

(defn sort-cube [cube]
  (sort
    (proxy [java.util.Comparator] []
      (compare [o1 o2]
        (compare (first o1) (first o2))))
    (partition 2 cube)))

(defn cube2csv [cube] ;2d only of course
  (let [sorted-cube (sort-cube cube)
        [[N _] _] (last sorted-cube)]
    (loop [rows (partition (inc N) sorted-cube)]
      (when-first [row rows]
        (println
          (apply str (drop-last (apply str (map #(str (second %1) ",") row)))))
        (recur (next rows))))))

(cube2csv expected)

(defmacro <all
  "([x & ys]
  Returns true if for each y in ys, (< x y) is true."
  [x & body]
  `(every? #(< ~x %1) ~@body))

(macroexpand-1 '(<all 1 [2 3 4]))
(<all 1 [2 3 4])
(<all 1 [2 0 4])

;summing
(def op +)
(def arc-op -)

(defn insert-into-cube
  "Build cube iteratively, returns new cube."
  [cube origin [cell value]]
  (println (meta cube))
  (let [N ((meta cube) :N)
        location (where-is-cell cell origin N)]
    (cond
      (= location :outside)
        (let [newcube (with-meta cube (assoc (meta cube) :N
                                         (loop [loc location n N]
                                           (if-let [newn (and
                                                           (= location :outside)
                                                           (* 2 (inc n)))]
                                             (recur (where-is-cell cell
                                                                   origin new-n)
                                                    new-n)
                                             n))))]
          ; we "grew" cube to fit the data. We have to insert&fill in the border
          ; cells and anchor cell(s) to their appropriate values
          )
      (= location :border) ; only need to update border cells that
        ; include this one and exist after in any dimension until anchor is hit.
        ; that is, 
      (= location :anchor) ; anchors are the sum from origin to anchor
        ; of original data, but cube is ddc. anchors at the origin are
        ; themselves. Anchors not at the origin are computed by
        ; value + .
      
      )))


(defn insert-ddc [cube N [cell value] origin]
  ; assert that cell is within NxN
  (cond
    (= cell origin) (conj cube cell value) ; origins are themselves

    ; if cell is in current layer, it's either an anchor or border cell
    (anchor? cell origin N)
      (let [newval 0]
        (conj cube [cell newval]))
      ; need sum from origin to cell, store that + value at cell for cube...
      ; (grab dependent anchor cell + dependent edge-of-border cell)
    (border? cube cell origin) 1
      ; get dependent sum, typically from one-less neighbor cell of the same
      ; dimension + yourself + ...
    :else
      (let [box-origin ]
        (recur cube (dec (/ N 2)) [cell value] box-origin))
    ; since cell is not in this layer, pick the box it's in and go down
    ;find which box the cell belongs in and recurse into that one
      ; with the new origin. worst-case we try inserting into a leaf which
      ; is its own anchor
  ))

(defn manhattan-dist [v1 v2]
  (apply + (map #(Math/abs (- %1 %2)) v1 v2)))

(defn anchor-slots [origin N] ; size 2^dim of origin
  (apply combin/cartesian-product (map #(range %1 (+ %1 N) (/ N 2)) origin)))
(def anchor-slots (memoize anchor-slots))

(defn gen-centers [anchors length]
  (map (fn [anchor] (map #(+ length %1) anchor)) anchors))
(def gen-centers (memoize gen-centers))


(defn where-is-cell "http://www.youtube.com/watch?v=JwRzi-E1l40" [cell origin N]
  (let [anchors (anchor-slots origin N)
        length (/ N 4.0)
        in-hypercube? (fn [center] ; returns the given center point of
                        ; a hypercube if the a cell is within its boundaries
                        ; (assuming its boundaries are N/4 in length in each
                        ; dimension)
                        (let [diffs (map #(Math/abs (- %1 %2)) cell center)]
                          (if (not-any? #(>= %1 length) diffs)
                            ; this is the center that this cell belongs in
                            center
                            ; else we're in a border-cell
                            false)))]
    (if (some #(>= %1 N) cell) :outside
      (if (not= -1 (.indexOf anchors cell))
        :anchor
        ; check quadrants in parallel, if nil, it's a border cell
        (if-let [center-q (some identity (pmap in-hypercube?
                                               (gen-centers anchors length)))]
          center-q
          :border)))))
(def where-is-cell (memoize where-is-cell))

;(def testcube ^{:N 1 :dimensions [:x :y]} [[0 0] 3]) ; initial 1x1

(def testcube ^{:N 1 :dimensions [:x :y]} [])

(insert-cube testcube [0 0] [[0 0] 3])

(insert-cube testcube [0 0] [[0 1] 5])
(insert-cube testcube [0 0] [[0 2] 1])
(insert-cube testcube [0 0] [[0 3] 2])
(insert-cube testcube [0 0] [[0 4] 2])

(insert-cube testcube [0 0] [[0 5] 4])

(insert-cube testcube [0 0] [[0 6] 6])
(insert-cube testcube [0 0] [[0 7] 3])
(insert-cube testcube [0 0] [[0 8] 3])

(insert-cube testcube [0 0] [[0 9] 1])

(def allbut00 (drop 1 (for [x (range 10) y (range 10)] [x y])))

(pprint (partition 10 (map #(list %1 "=>" (where-is-cell %1 [0 0] 10)) allbut00)))

(pprint (partition 4 (map #(list %1 "=>" (where-is-cell %1 [1 6] 4)) (for [x (range 1 5) y (range 6 10)] [x y]))))

(reduce (fn [cube coord]
          (comment (print coord (query-cube orig coord) "; "))
          (insert-cube cube [coord (query-cube orig coord)]))
        testcube
        allbut00)


(defn add2cube [[cell value] cube]
  (conj cube cell value)
  )

(loop [rows (partition 10 (partition 2 orig)) cube test2]
  (when-first [row rows]
    (println row)
    (read)
    (println (map #(do (println %1 (add2cube %1 cube)) *1) row))
    (recur (next rows) cube)))

(def raw-data ^{:dimensions [:airline [:year :qtr] :origin] :measure :qty}
  [[:alaska [2010 :q1] :ca] 2
   [:alaska [2010 :q1] :wa] 2
   [:alaska [2010 :q2] :ca] 5
   [:alaska [2010 :q2] :or] 4
   [:alaska [2010 :q2] :wa] 3
   [:alaska [2010 :q3] :ca] 2
   [:alaska [2010 :q4] :or] 2
   [:alaska [2011 :q1] :wa] 4
   [:alaska [2011 :q2] :ca] 2
   [:delta [2010 :q1] :ca] 1
   [:delta [2010 :q2] :wa] 3
   [:delta [2010 :q3] :ca] 4
   [:delta [2010 :q4] :or] 2
   [:delta [2011 :q1] :wa] 4
   [:united [2010 :q4] :or] 2
   [:united [2011 :q1] :ca] 1
   [:united [2011 :q1] :or] 2
   [:united [2011 :q1] :wa] 4])

(defn query-cube
  [cube query]
  (let [[k v] (first (filter #(= query (first %1)) (partition 2 cube)))]
    v))

(query-raw-data [:delta [2011 :q1] :wa])

(defn query2 [query]
  (let [coords (take-nth 2 raw-data)
        values (take-nth 2 (drop 1 raw-data))]
    (nth values (.indexOf coords query))))
(query2 [:delta [2011 :q1] :wa])

