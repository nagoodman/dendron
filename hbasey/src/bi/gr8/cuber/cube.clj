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

(def ^:dynamic *maxdim* 10)

;;;;;;;;;;;;;;;;;;
; querying-related
;;;;;;;;;;;;;;;;;;

(defn matching-borders
  ([cell anchor]
    (map (fn [[i v]] (assoc anchor i v)) (map-indexed vector cell)))
  ([cell anchor dim]
    (if (zero? dim) (matching-borders cell anchor)
      (let [idxs (range (count anchor))
            idx-groups (combin/combinations idxs (inc dim))]
        (map (fn [group] (apply assoc anchor (flatten (map #(vector %1 (nth cell %1)) group)))) idx-groups)))))
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
  (let [res (mapper (fn [[dim name]] (read-name2key keytab dim name))
                    (map-indexed vector namedcell))]
    (if *noisy?* (println "Converted" namedcell "to" res))
    res))
(def names2nums (memoize names2nums))

(defn query [cube keytab cell N kind] ; set kind=:sum
  (let [dimwise-level-anchors (map #(relative-anchors N %1) cell)
        dims (count dimwise-level-anchors)
        anchors (partition dims (apply interleave dimwise-level-anchors))]
    (if *noisy?* (println "anchors involved" anchors))
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
;;;;;;;;;;;;;;;;;;
; creation related
;;;;;;;;;;;;;;;;;;

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
  (every? identity (mapper (fn [[dim k]] (read-key2name keytab dim k))
                           (map-indexed vector cell))))
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

(defn where-is-cell "http://www.youtube.com/watch?v=JwRzi-E1l40" [cell origin N]
  (let [anchors (anchor-slots origin N)
        d-1 (dec (count origin))
        length (/ N 4)
        in-hypercube? (fn [center] ; returns the given center point of
                        ; a hypercube if the a cell is within its boundaries
                        ; (assuming its boundaries are N/4 in length in each
                        ; dimension)
                        (let [diffs (map keydist cell center)]
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
        (if-let [center-q (some identity (map in-hypercube?
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
          (if *noisy?* (println cell :anchor))
          (dorun (mapper (fn [anchor]
                           (if (cell-in-keys? keytab anchor)
                             (store-val cube anchor data kind)))
                         anchors-to-update)))
      (= location :border)
        (let [anchors-to-update (get-dependent-anchors origin cell N)
              border-to-update (get-opposing-border origin origin cell N)
              ;other-borders* (matching-borders border-to-update origin 1)
              ;_ (println other-borders*)
              ;other-borders (filter (fn [bord] (and (not= cell bord) (every? (fn [[ai ci]] (>= ai ci)) (partition 2 (interleave bord cell))))) other-borders*)
              ;_ (println other-borders)
              to-updates (if (not= cell border-to-update)
                          (conj anchors-to-update border-to-update cell)
                          (conj anchors-to-update border-to-update))]
          (if *noisy?* (println cell :border))
          (dorun (mapper (fn [to-update]
                           (if (cell-in-keys? keytab to-update)
                             (store-val cube to-update data kind)))
                         to-updates)))
                       ;(concat to-updates other-borders))))
      :else
        (let [anchors-to-update (get-dependent-anchors origin cell N)
              anchor (map #(long (- %1 %2)) location (repeat (/ N 4)))
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

;;;;;;;;;;;;;;;;;;
; last step, sum up border cells
;;;;;;;;;;;;;;;;;;

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

(defn get-border-cells-improved
  "For a particular anchor cell, get its corresponding border cells that exist
  in a sorted lazy seq. It works by holding the first dimension, then for
  the other dimensions iterating to the N val in that dimension. It does not
  compute non-existent border cells known from Ns."
  [anchor Ns]
  (let [halfs (map #(/ %1 2) Ns)
        anch (set anchor)
        values (map #(range %1 (+ %1 half)) anchor)]
    (filter #(> (count (clojure.set/intersection anch (set %1))) 0)
            (apply combin/cartesian-product values))))

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
    (if *noisy?* (do (println "left:" left-bound "diff:" differing-dims "add:" to-add "sub:" to-sub)))
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

(defn sum-borders
  "Warning: intractible for dense cubes.
  Counts represent the number of
  unique keys per dimension."
  [cube keytab origin N counts]
  ; we go level-by-level.
  (let [dims (count origin)
        Ns (map calculate-N counts)
        levels (log2 N)
        k (dec (/ N 2))]
    (println "Levels to go from" origin ":" (dec levels))
    (if (> levels 0)
      ; for every level, we have 2^d "boxes" where each dim is length k
      ; and the anchor is the box-origin for sub-boxes
      (dorun (mapper
               (fn [box-origin]
                 (if (cell-in-keys? keytab box-origin)
                   (let [bord-cells (get-border-cells box-origin N)
                         _ (comment bord-cells (keep identity
                                          (mapper #(if (cell-in-keys? keytab %1)
                                                     %1
                                                     nil)
                                                  bord-cells*)))]
                     (if *noisy?* (println "bord-cells:" bord-cells))
                     (dorun (map (fn [cell]
                                   (let [val (calc-bord-sum-value cube cell box-origin)]
                                     (if (not (zero? val))
                                       (store-val cube cell val :sum))))
                                 bord-cells))
                     ; then, recursively apply this algorithm
                     ; in another thread.
                     (future (sum-borders cube keytab (map inc box-origin) k))
                     )))
               (anchor-slots origin N))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; meta-data key-creation related
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn calculate-N [limit]
  (loop [N 4]
    (if (< N limit)
      (recur (* 2 (inc N)))
      N)))

(defn read-csv-line [line]
  (take *maxdim* (.split line ",")))

; for initial key-load
(defn create-key-int-map [files keytab]
  (let
    [sets (doall (map (fn [file]
      (if *noisy?* (println "Reading" file))
      (with-open [rdr (reader file)]
        (let [firstline (read-csv-line (first (line-seq rdr)))
              last-datum? (try (Long/parseLong (last firstline)) true
                            (catch Exception e false))
              keyset (loop [lines (line-seq rdr) md-keyset []]
                       (if (seq lines)
                         (let [line (read-csv-line (first lines))
                               cell (if last-datum? (drop-last line) (seq line))]
                           (recur (next lines)
                                  (vec (map-indexed #(clojure.set/union
                                                       (get md-keyset %1)
                                                       (sorted-set %2))
                                                    cell))))
                         md-keyset))]
          (if *noisy?* (println "Finished reading" file))
          keyset)))
                      files))
        ; merge keysets..
     merged-keyset (reduce (fn [set1 set2] (map clojure.set/union set1 set2)) sets)
     _ (dorun (map #(assert (= clojure.lang.PersistentTreeSet (class %1))) merged-keyset))
     counts (doall (map count merged-keyset))
     N (calculate-N (apply max counts))]
    (println "dims, unique keys per dimension, N:" (count counts) counts N)
    (let [key-promises
          (map (fn [[dim keys]]
                 (map
                   (fn [[idx name]]
                     (future
                       (store-raw keytab (str dim "-dimkey-" idx) name)
                       (store-raw keytab (str dim "-namekey-" name) idx)))
                   (map-indexed vector keys)))
               (map-indexed vector merged-keyset))]
      ; wait for them to finish
      (doseq [[dim promise] (seque 30 (map-indexed vector key-promises))]
        (println "Awaiting dim" dim)
        (dorun (pmap deref promise))
        (println "Finished dim" dim)))
    (future
      (println "Warming up the key-name cache in the background...")
      (dorun (map (fn [[dim keys]]
                  (dorun (map (fn [[idx name]]
                         (read-name2key keytab dim name)
                         (read-key2name keytab dim idx))
                       (map-indexed vector keys))))
                (map-indexed vector merged-keyset)))
      (println "Finished warming up the key-name cache."))
    ; return origin and N after storing
    (let [origin (vec (take (count counts) (repeat 0)))]
      (store-origin-N keytab origin N counts)
      [origin N counts])))

(defn insert-row-by-row [cube keytab origin datafile N]
  (with-open [rdr (reader datafile)]
    (let [firstline (read-csv-line (first (line-seq rdr)))
          last-datum? (try (Long/parseLong (last firstline)) true
                        (catch Exception e false))
          adder-promises
          (map-indexed
            (fn [idx line]
              (let [line-data (read-csv-line line)
                    [value cell] (if last-datum?
                                   [(last line-data) (drop-last line-data)]
                                   [1 (seq line-data)])]
                (if *noisy?* (println "Adding" [cell value] "to cube..."))
                (future
                  (add-row-to-cube cube keytab origin [cell value] N :sum))))
            (seque 100000 (line-seq rdr)))]
      (dorun (pmap (fn [[idx promise]]
                     (deref promise)
                     (if (= 0 (mod idx 5000))
                       (println "Finished line" idx "at"
                                (str (java.util.Date.)))))
                   (seque 30 (map-indexed vector adder-promises))))))
  (println "Done inserting for" datafile))


