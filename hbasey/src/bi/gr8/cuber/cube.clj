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
        [clojure.repl]
        [clojure.java.io]))

;(binding [*noisy?* true] (compile 'bi.gr8.cuber.cube))
(defmacro mapper-chooser []
  (if *noisy?*
    (do (println "map") `map)
    (do (println "pmap") `pmap)))

(def mapper (mapper-chooser))

; querying-related

;(matching-borders [:x :y :z :w] [:a :b :c :d] 3)

(defn matching-borders
  ([cell anchor]
    (map (fn [[i v]] (assoc anchor i v)) (map-indexed vector cell)))
  ([cell anchor dim]
    (if (zero? dim) (matching-borders cell anchor)
      (let [idxs (range (count anchor))
            idx-groups (combin/combinations idxs (inc dim))]
        (map (fn [group] (apply assoc anchor (flatten (map #(vector %1 (get cell %1)) group)))) idx-groups)))))
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
(def relative-anchors (memoize relative-anchors))


(defn names2nums [keytab namedcell]
  (let [res (map (fn [[dim name]] (read-name2key keytab dim name))
                 (map-indexed vector namedcell))]
    (if *noisy?* (println "Converted" namedcell "to" res))
    res))
(def names2nums (memoize names2nums))

(defn query [cube keytab cell N kind] ; set kind=:sum
  (let [dimwise-level-anchors (map #(relative-anchors N %1) cell)
        dims (count dimwise-level-anchors)
        anchors (partition dims (apply interleave dimwise-level-anchors))]
    (reduce +
            (mapper
              (fn [anch]
                (let [anchor (vec anch)
                      bords (apply concat
                                   (map-indexed #(matching-borders cell %2 %1)
                                                (take (dec dims)
                                                      (repeat anchor))))]
                  (reduce + (mapper #(do (read-val cube %1 kind)) (set (conj bords anchor))))))
              anchors))))

; creation related

(defn anchor-slots
  "Returns the 2^dim anchors with shared origin"
  [origin N]
  (apply combin/cartesian-product (map #(range %1 (+ %1 N) (/ N 2)) origin)))
(def anchor-slots (memoize anchor-slots))

(defn get-dependent-anchors
  "Returns any anchors whose value is derived in part from cell,
  including cell itself if cell is an anchor."
  [origin cell N]
  (if (= N 1) (list cell)
    (let [anchors (anchor-slots origin N)]
      (filter (fn [anchor] (every? (fn [[ai ci]] (>= ai ci)) (partition 2 (interleave anchor cell)))) anchors))))
(def get-dependent-anchors (memoize get-dependent-anchors))

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
(def cell-in-keys? (memoize cell-in-keys?))

(defn intersecting-borders [cell anch]
  ; for each dimension of cell, start from the anchor and travel
  ; in the current dimension until the keys are the same, and take this
  ; cell.
  (let [anchor (vec anch)]
    (keep-indexed (fn [idx dim]
                    (let [oldval (nth anchor idx)]
                      (assoc anchor idx (+ oldval (- dim oldval)))))
                  cell)))
(def intersecting-borders (memoize intersecting-borders))

(defn keydist
  "Given 2 keys in the same dimension, how 'far' apart are they?"
  [key1 key2]
  (Math/abs (double (- key1 key2))))

(defn gen-centers [anchors length]
  (map (fn [anchor] (map #(+ length %1) anchor)) anchors))
(def gen-centers (memoize gen-centers))

; this function seems to have difficulties sometimes in 3+ dimensions... I'm seeing
; an off-by-x error.
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
        (if-let [center-q (some identity (mapper in-hypercube?
                                               (gen-centers anchors length)))]
          center-q
          :border)))))
(def where-is-cell (memoize where-is-cell))

; called for each row
(defn add-row-to-cube ; set kind=:sum
  [cube keytab origin row N kind]
  (let [;lastval (agent 0) ; dummy agent to support async updates
        ; it's actually slower in a test. to try again,
        ; simply change (store-val ..) to (send-off lastval store-val-async ..)
        [namedcell data] row
        cell (names2nums keytab namedcell)
        location (where-is-cell cell origin N)]
    (cond
      (= location :anchor)
        (let [anchors-to-update (get-dependent-anchors origin cell N)]
          (if *noisy?* (println cell :anchor))
          (dorun (mapper (fn [anchor]
                         (if (cell-in-keys? keytab anchor)
                           (store-val cube anchor data kind)))
                       anchors-to-update)))
      (= location :border)
        (let [anchors-to-update (get-dependent-anchors origin cell N)
              border-to-update (get-opposing-border origin origin cell N)
              to-updates (if (not= cell border-to-update)
                          (conj anchors-to-update border-to-update cell)
                          (conj anchors-to-update border-to-update))]
          (if *noisy?* (println cell :border))
          (dorun (mapper (fn [to-update]
                         (if (cell-in-keys? keytab to-update)
                           (store-val cube to-update data kind)))
                       to-updates)))
      :else
        (let [anchors-to-update (get-dependent-anchors origin cell N)
              anchor (map #(int (- %1 %2)) location (repeat (/ N 4)))
              intersecting-borders (intersecting-borders cell anchor)
              borders-to-update (filter identity
                                        (map #(let [ret (get-opposing-border
                                                          origin anchor %1 N)]
                                                (if (= ret %1) nil ret))
                                             intersecting-borders))]
          (if *noisy?* (println cell :recur location))
          (dorun (mapper (fn [to-update]
                         (if (cell-in-keys? keytab to-update)
                           (store-val cube to-update data kind)))
                       (concat anchors-to-update borders-to-update)))
          (add-row-to-cube cube keytab (map + anchor (repeat 1)) row (dec (/ N 2)) kind))
      )))
    ;(await lastval)))

; for the final step of summing border regions

;called at the end of the initial cube-creation process

(defn calculate-N [limit]
  (loop [N 4]
    (if (< N limit)
      (recur (* 2 (inc N)))
      N)))

; for initial key-load
(defn create-key-int-map [files keytab]
  (let [sets (pmap
            (fn [file]
              (with-open [rdr (reader file)]
                (let [firstline (.split (first (line-seq rdr)) ",")
                      last-datum? (try (Integer/parseInt (last firstline)) true
                                    (catch Exception e false))
                      keyset (loop [lines (line-seq rdr) md-keyset []]
                               (if (seq lines)
                                 (let [line (.split (first lines) ",")
                                       cell (if last-datum? (drop-last line) (seq line))]
                                   (recur (next lines)
                                          (vec (map-indexed #(clojure.set/union (get md-keyset %1)
                                                                                (sorted-set %2))
                                                            cell))))
                                 md-keyset))]
                  keyset)))
            files)
        ; merge keysets..
        merged-keyset (reduce clojure.set/union sets)
        _ (map #(assert (= clojure.lang.PersistentTreeSet (class %1))) merged-keyset)
        counts (map count merged-keyset)
        N (calculate-N (apply max counts))]
    (println "stored" (map count (pmap (fn [[dim keys]]
                                   (pmap (fn [[idx name]]
                                           (store-raw keytab (str dim "-dimkey-" idx) name)
                                           (store-raw keytab (str dim "-namekey-" name) idx))
                                         (map-indexed vector keys)))
                                 (map-indexed vector merged-keyset)))
           "keypairs")
    (println "unique keys per dimension:" counts)
    ; return origin and N
    [(vec (take (count counts) (repeat 0))) N]))

(defn insert-row-by-row [cube keytab origin datafile N]
  (with-open [rdr (reader datafile)]
    (let [firstline (.split (first (line-seq rdr)) ",")
          last-datum? (try (Integer/parseInt (last firstline)) true
                        (catch Exception e false))]
      (dorun (pmap (fn [line]
                     (let [line-data (.split line ",")
                           [value cell] (if last-datum?
                                          [(last line-data) (drop-last line-data)]
                                          [1 (seq line-data)])]
                       (if *noisy?* (println "Adding" [cell value] "to cube..."))
                       (add-row-to-cube cube keytab origin [cell value] N :sum)))
                   (line-seq rdr)))))
  (println "Done inserting for" datafile))


; better border-sum...

(defn get-border-cells
  "For a particular anchor cell, get its corresponding border cells in
  a sorted lazy seq. It works by holding the first dimension, then for
  the other dimensions iterating to the N val in that dimension."
  [anchor N]
  (let [half (/ N 2)
        anch (set anchor)
        values (map #(range %1 (+ %1 half)) anchor)]
    (filter #(> (count (clojure.set/intersection anch (set %1))) 0)
            (apply combin/cartesian-product values))))
(def get-border-cells (memoize get-border-cells))

(defn calc-bord-sum-value
  "Pre-condition 1: all dimensions obey the constraint that at least
  one matches the corresponding anchor dimension and the rest
  satisfy a_i + 1 <= c_i < a_i + k with k being (dec (/ N 2)).
  Pre-condition 2: a given cell must have their immediately lesser
  neighbor(s) already computed.
  "
  [cube cell anchor]
  (let [cell (vec cell) anchor (vec anchor)
        left-bound (map-indexed #(if (= (get cell %1) %2) %2 (inc %2)) anchor)
        differing-dims (keep-indexed #(if (not= (get cell %1) %2) %1) left-bound)
        to-add (map #(assoc cell %1 (dec (get cell %1))) differing-dims)
        to-sub (reduce #(assoc %1 %2 (dec (get cell %2))) cell differing-dims)]
    (if *noisy?* (do (println "add:" to-add "sub:" to-sub)))
    (cond
      (= to-sub cell) 
        0 ; neighbor to anchor
      (= to-sub (first to-add))
        (read-val cube to-sub :sum) ; left-bound "1 away"
      ; else "more than 1 away"
      :else (apply +
                   (conj (mapper #(read-val cube %1 :sum) to-add)
                         (* -1 (read-val cube to-sub :sum)))))))

(defn log2 [x]
  (long (Math/floor (/ (Math/log x) (Math/log 2)))))

(defn sum-borders [cube keytab origin N]
  ; we go level-by-level.
  (let [dims (count origin)
        levels (log2 N)
        k (dec (/ N 2))]
    (if (> levels 0)
      ; for every level, we have 2^d "boxes" where each dim is length k
      ; and the anchor is the box-origin for sub-boxes
      (dorun (mapper
               (fn [box-origin]
                 (let [bord-cells (drop 1 (get-border-cells box-origin N))]
                   (if *noisy?* (println "bord-cells:" bord-cells))
                   (dorun (map (fn [cell]
                                 (if (cell-in-keys? keytab cell)
                                   (store-val cube cell
                                              (calc-bord-sum-value cube cell box-origin)
                                              :sum)))
                               bord-cells))
                   ; then, recursively apply this algorithm.
                   (sum-borders cube keytab box-origin k)
                   ))
               (anchor-slots origin N))))))
