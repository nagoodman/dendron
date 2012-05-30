(ns bi.gr8.cuber.cube
  "Methods for creating, querying, and updating the 'cube'."
  (:import [java.util UUID]
           [org.apache.hadoop.hbase.util Bytes])
  (:require [clojure-hbase.core :as hb]
            [clojure-hbase.admin :as hba]
            [rotary.client :as dyndb]
            [clojure.math.combinatorics :as combin]
            [clojure.set])
  (:use [bi.gr8.cuber storage]
        [clojure.pprint]
        [clojure.java.io]))

(defn log2 [x]
  (long (Math/floor (/ (Math/log x) (Math/log 2)))))


; querying-related

(defn matching-borders [cell anchor]
  (map (fn [[i v]] (assoc anchor i v)) (map-indexed vector cell)))
(def matching-borders (memoize matching-borders))

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


(defn names2nums [keytab namedcell]
  (map (fn [[dim name]] (read-name2key keytab dim name))
    (map-indexed vector namedcell)))
(def names2nums (memoize names2nums))

(defn query [cube keytab cell N kind] ; set kind=:sum
  (let [dimwise-level-anchors (map #(relative-anchors N %1) cell)
        dims (count dimwise-level-anchors)
        anchors (partition dims (apply interleave dimwise-level-anchors))]
    (println "cell:" cell "anchors:" anchors)
    (reduce +
            (pmap
              (fn [anch]
                (let [anchor (vec anch)
                      bords (matching-borders cell anchor)]
                  (reduce + (pmap #(do (read-val cube %1 kind)) (set (conj bords anchor))))))
              anchors))))

; creation related

(defn anchor-slots [origin N] ; size 2^dim of origin
  (apply combin/cartesian-product (map #(range %1 (+ %1 N) (/ N 2)) origin)))
(def anchor-slots (memoize anchor-slots))

(defn get-dependent-anchors [origin cell N]
  ; includes cell itself if it's an anchor
  (if (= N 1) (list cell)
    (let [anchors (anchor-slots origin N)]
      (filter (fn [anchor] (every? (fn [[ai ci]] (>= ai ci)) (partition 2 (interleave anchor cell)))) anchors))))

(defn get-opposing-border [origin anchor cell N]
  (map #(let [sum (+ %2 (/ N 2))]
          (if (and (= %1 %2)
                   (<= sum (+ %3 (/ N 2))))
            sum
            %2))
    anchor cell origin))
(def get-opposing-border (memoize get-opposing-border))

(defn cell-in-keys? [keytab cell]
  (every? (fn [[dim k]] (read-key2name keytab dim k))
    (map-indexed vector cell)))
;(def cell-in-keys? (memoize cell-in-keys?))

(defn intersecting-borders [cell anch]
  ; for each dimension of cell, start from the anchor and travel
  ; in the current dimension until the keys are the same, and take this
  ; cell.
  (let [anchor (vec anch)]
    (keep-indexed (fn [idx dim]
                    (let [oldval (nth anchor idx)]
                      (assoc anchor idx (+ oldval (- dim oldval)))))
                  cell)))

(defn keydist
  "Given 2 keys in the same dimension, how 'far' apart are they?"
  [key1 key2]
  (Math/abs (double (- key1 key2))))

(defn gen-centers [anchors length]
  (map (fn [anchor] (map #(+ length %1) anchor)) anchors))
(def gen-centers (memoize gen-centers))

(defn where-is-cell "http://www.youtube.com/watch?v=JwRzi-E1l40" [cell origin N]
  (let [anchors (anchor-slots origin N)
        d-1 (dec (count origin))
        length (/ N 4)
        in-hypercube? (fn [center] ; returns the given center point of
                        ; a hypercube if the a cell is within its boundaries
                        ; (assuming its boundaries are N/4 in length in each
                        ; dimension)
                        (let [diffs (map keydist cell center)]
                          ; not-any may be wrong; no more than d-1?
                          ;(if (< (count (filter #(>= %1 length) diffs)) d-1)
                          (if (not-any? #(>= %1 length) diffs)
                            ; this is the center that this cell belongs in
                            center
                            ; else we're in a border-cell
                            false)))]
    (if (some #(>= %1 N) (map - cell origin)) :outside
      (if (not= -1 (.indexOf anchors cell))
        :anchor
        ; check quadrants in parallel, if nil, it's a border cell
        (if-let [center-q (some identity (pmap in-hypercube?
                                               (gen-centers anchors length)))]
          center-q
          :border)))))
(def where-is-cell (memoize where-is-cell))

; called for each row
(defn add-row-to-cube ; set kind=:sum
  [cube keytab origin row N kind]
  (let [[namedcell data] row
        cell (names2nums keytab namedcell)
        location (where-is-cell cell origin N)]
    (cond
      (= location :anchor)
        (let [anchors-to-update (get-dependent-anchors origin cell N)]
          (doseq [anchor anchors-to-update]
            (if (cell-in-keys? keytab anchor)
              (store-val cube anchor data kind))))
      (= location :border)
        (let [anchors-to-update (get-dependent-anchors origin cell N)
              border-to-update (get-opposing-border origin origin cell N)
              to-updates (if (not= cell border-to-update)
                          (conj anchors-to-update border-to-update cell)
                          (conj anchors-to-update border-to-update))]
          (doseq [to-update to-updates]
            (if (cell-in-keys? keytab to-update)
              (store-val cube to-update data kind))))
      :else
        (let [anchors-to-update (get-dependent-anchors origin cell N)
              anchor (map #(int (- %1 %2)) location (repeat (/ N 4)))
              intersecting-borders (intersecting-borders cell anchor)
              borders-to-update (filter identity
                                        (map #(let [ret (get-opposing-border
                                                          origin anchor %1 N)]
                                                (if (= ret %1) nil ret))
                                             intersecting-borders))]
          (doseq [to-update (concat anchors-to-update borders-to-update)]
            (if (cell-in-keys? keytab to-update)
              (store-val cube to-update data kind)))
          (add-row-to-cube cube keytab (map + anchor (repeat 1)) row (dec (/ N 2)) kind))
      )))

; for the final step of summing border regions

(defn get-all-border-regions [origin N]
  ; return a seq of all pairs of border regions in a cube
  ; (with the start offset by 1 so that we can start by reading,
  ; increment, increment, ...)
  (if (> N 4)
    (let [anchors (map vec (anchor-slots origin N))
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
(defn sum-cube-borders [cube keytab origin N]
  (let [dim (count origin)
        bord-regs (get-all-border-regions origin N)]
    (map (fn [box]
           (map-indexed (fn [idx [start end]]
                          (if (cell-in-keys? keytab start)
                            (loop [cell start, value (read-val cube start :sum)]
                              (if (and (< (nth cell idx)
                                          (nth end idx))
                                       (or value (not= cell start)))
                                (let [next-cell (assoc cell idx (inc (nth cell idx)))]
                                  (if (cell-in-keys? keytab next-cell)
                                    (recur next-cell
                                           (store-val cube next-cell value :sum))))))))
                        box))
         bord-regs)
    ))

(defn calculate-N [limit]
  (loop [N 4]
    (if (< N limit)
      (recur (* 2 (inc N)))
      N)))

; for initial key-load
(defn create-key-int-map [files keytab]
        dims (count (first lines))
  (doseq [file files]
    (with-open [rdr (reader file)]
      (doseq [line (line-seq rdr)]
        (let [cell (seq (.split line ","))
              keyset (loop [md-keyset []]
                       (recur (vec (map-indexed #(clojure.set/union (get md-keyset %1) (sorted-set %2)) cell))))
                   md-keyset
        counts (map count keyset)
        N (calculate-N (apply max counts))]
    (doseq [[dim keys] (map-indexed vector keyset)]
      (doseq [[idx name] (map-indexed vector keys)]
        ;(store-raw keytab name [(str dim "-dimkey") idx])
        ;(store-raw keytab idx [(str dim "-namekey") name])

        ;new
        (store-raw keytab (str dim "-dimkey-" idx) k)
        ;(println k "=>" idx)
        (store-raw keytab (str dim "-namekey-" k) idx)
        ))
    (println counts N)))

(defn insert-row-by-row [cube keytab origin datafile N]
  (with-open [rdr (reader datafile)]
    (doseq [line (line-seq rdr)]
      (let [cell (seq (.split line ","))
            value 1] ; should come from somewhere...
        (add-row-to-cube cube keytab origin [cell 1] N :sum)))))


; better border-sum...
(comment
(defn sum-borders []
  ; we go leve-by-level.
  (loop [level 0]
    ; for every level, we have four "boxes" of d dimensions.
    ; so, pmap 4 times.
    (pmap (fn [box]
            ; for each box, sum borders in each of d dimensions.
            ; pmap the dims.
            (pmap (fn [] 
                    ; summing is simple...
                    )  (range 2))
            ; then, recursively apply this algorithm.
            ) (range 5))

    ))
)
