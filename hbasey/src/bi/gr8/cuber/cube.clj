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

(defn calculate-N [limit]
  (loop [N 4]
    (if (< N limit)
      (recur (* 2 (inc N)))
      N)))
(def calculate-N (memoize calculate-N))

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
              other-borders* (apply concat (map #(intersecting-borders %1 cell) anchors-to-update))
              other-borders (concat other-borders* (apply concat (map #(intersecting-borders (take (count origin) (repeat (/ N 2))) %1) (concat [cell] [border-to-update]))))
              to-updates (conj anchors-to-update border-to-update cell)]
          (if *noisy?* (println cell :border))
          (dorun (mapper (fn [to-update]
                           (if (and (every? true? (map <= cell to-update))
                                    (cell-in-keys? keytab to-update)
                                    (keyword? (where-is-cell to-update origin N)))
                             (store-val cube to-update data kind)))
                       (set (concat to-updates other-borders)))))
      :else
        (let [
              anchors-to-update (get-dependent-anchors origin cell N)
              b-border-to-update (get-opposing-border origin origin cell N)
              b-other-borders* (apply concat (map #(intersecting-borders %1 cell) anchors-to-update))
              b-other-borders (concat b-other-borders* (apply concat (map #(intersecting-borders (take (count origin) (repeat (/ N 2))) %1) (concat [cell] [b-border-to-update]))))
              anchor (map #(long (- %1 %2)) location (repeat (/ N 4)))
              intersecting-borders (intersecting-borders cell anchor)
              borders-to-update (filter identity
                                        (map #(let [ret (get-opposing-border
                                                          origin anchor %1 N)]
                                                (if (= ret %1) nil ret))
                                             intersecting-borders))]
          (if *noisy?* (println cell :recur location))
          (dorun (mapper (fn [to-update]
                           (if (and (every? true? (map <= cell to-update))
                                    (cell-in-keys? keytab to-update)
                                    (keyword? (where-is-cell to-update origin N)))
                             (store-val cube to-update data kind)))
                         (set (concat anchors-to-update borders-to-update b-other-borders))))
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

(comment

(let [anchor [0 0 0] N 4 half (/ N 2)]
  (map #(range %1 (+ %1 half)) anchor))



(time (count (get-border-cells-improved [0 0 0 0 0 0 0 0 0 0] (calculate-N 6000) [10 1 1 10 3000 12 1 12 12 1])))

(time (count (get-border-cells-no-filter [0 0 0 0 0 0 0 0 0 0] (calculate-N 6000) [10 1 1 10 3000 12 1 12 12 1])))

(time (count (get-border-cells-imp [0 0 0 0 0 0 0 0 0 0] (calculate-N 6000) [10 1 1 10 3000 12 1 12 12 1])))


  (get-border-cells [0 0 0] 10)

(let [v-seqs [(range 5) (range 5) (range 1 5)]
      lst-one (last v-seqs)
      ]
  (if-let [rst (next lst-one)]
    (assoc 
(loop [i 2] 
  (if (= i -1) nil
    (if-let [rst (next (v-seqs i))]
      (assoc v-seqs i rst)
      (recur (dec i) (assoc v-seqs i ([(range 5) (range 5) (range 5)] i))))))

))))

;(time (count (doall (get-border-cells-improved [0 0 0 0] 190 [10 10 10 10]))))
;(time (count (doall (get-border-cells [0 0 0 0] 10))))

(defn pseudo-cartesian-product
  "All the ways to take one item from each sequence filtering out those that
  don't have a dimension matching with required."
  [required & seqs]
  (let [lst-idx (dec (count required))
        sec-lst-idx (dec lst-idx)
        lst-val (long (required lst-idx))
        v-orig-seqs (vec seqs)
        orig-first (first seqs)
        matches-a-dim? (fn [cell] (some true? (map = cell required)))
        increment (fn [v-seqs]
                    (loop [i lst-idx, v-seqs v-seqs]
                      (if (= i -1) nil
                        (if-let [rst (next (v-seqs i))]
                          (assoc v-seqs i rst)
                          (recur (dec i) (assoc v-seqs i (v-orig-seqs i)))))))
        step
        (fn step [v-seqs]
          (when-let [product (and v-seqs (map first v-seqs))]
            (if (matches-a-dim? product)
              (cons product (lazy-seq (step (increment v-seqs))))
              (if-let [rst (next (v-seqs sec-lst-idx))]
                (let [;others (lazy-seq
                      ;         (step (increment
                      ;                 (assoc v-seqs
                      ;                        lst-idx '(nil)
                      ;                        sec-lst-idx '(nil)))))
                      [nxt & rst] (pmap #(assoc (vec product)
                                                sec-lst-idx %1
                                                lst-idx lst-val)
                                        rst)]
                  (cons nxt (concat rst)))); others))))
              )))]
    (when (every? first seqs)
      (lazy-seq (apply concat (pmap (fn [dim1]
              (lazy-seq (step (assoc v-orig-seqs 0 (range dim1 (+ 2 dim1)))))
              )
            (conj (drop 2 orig-first) (first orig-first)))))
      ;(lazy-seq (step v-orig-seqs))
      )))

(defn pfilter [pred coll]
  (map second
    (filter first
      (pmap (fn [item] [(pred item) item]) coll))))

(defn get-border-cells-improved
  "For a particular anchor cell, get its corresponding border cells that exist
  in a sorted lazy seq. It works by holding the first dimension, then for
  the other dimensions iterating to the N val in that dimension. It does not
  compute non-existent border cells known from Ns."
  [anchor N Ns]
  (let [halfs (map #(min (/ N 2) (/ %1 2)) Ns)
        values (map-indexed #(range %2 (+ %2 (nth halfs %1))) anchor)]
    (apply pseudo-cartesian-product anchor values)))

(defn get-border-cells-imp
  [anchor N Ns]
  (let [halfs (map #(min (/ N 2) (/ %1 2)) Ns)
        values (map #(range %1 (+ %1 %2)) anchor halfs)]
    (apply concat (pmap (fn [cs] (pfilter #(some true? (map = anchor %1)) cs))
            (partition-all 512 (apply combin/cartesian-product values))))))

(defn get-border-cells-no-filter
  [anchor N Ns]
  (let [halfs (map #(min (/ N 2) (/ %1 2)) Ns)
        values (map-indexed #(range %2 (+ %2 (nth halfs %1))) anchor)]
    (apply combin/cartesian-product values)))

(defn memo-limited
  "Same as memoize but keeps a max of 5000 lookups."
  [f]
  (let [mem (atom {})]
    (fn [& args]
      (if (> (count @mem) 5000)
        (swap! mem #(into {} (drop 200 %1))))
      (if-let [e (find @mem args)]
        (val e)
        (let [ret (apply f args)]
          (swap! mem assoc args ret)
          ret)))))

(def read-bord-val (memo-limited read-val))

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
    (if *noisy?* (println "left:" left-bound "diff:" differing-dims "add:" to-add "sub:" to-sub))
    (cond
      (= to-sub cell) 
        0 ; neighbor to anchor
      (= to-sub (first to-add))
        (read-bord-val cube to-sub :sum) ; left-bound "1 away"
      ; else "more than 1 away"
      :else (apply +
                   (conj (mapper #(read-bord-val cube %1 :sum) to-add)
                         (* -1 (read-bord-val cube to-sub :sum)))))))

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
    (println "Levels to go from" origin ":" (dec levels)
             "(" (str (java.util.Date.)) ")")
    (if (> levels 0)
      ; for every level, we have 2^d "boxes" where each dim is length k
      ; and the anchor is the box-origin for sub-boxes
      (dorun (mapper
               (fn [box-origin box-num]
                 (if (cell-in-keys? keytab box-origin)
                   (let [bord-cells (get-border-cells-imp box-origin N Ns)]
                     (dorun (map (fn [cell bci]
                     (if (zero? (mod bci 50000)) (println "bci:" bci
                                                          origin
                                                          (str (java.util.Date.))))
                                   (let [val (calc-bord-sum-value cube cell box-origin)]
                                     (if (not (zero? val))
                                       (store-val cube cell val :sum))))
                                 bord-cells (iterate inc 0)))
                     ; then, recursively apply this algorithm
                     ; in another thread.
                     (future (sum-borders cube keytab (map inc box-origin) k counts))
                     )))
               (anchor-slots origin N) (iterate inc 0))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; meta-data key-creation related
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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
                     (if (= 0 (mod idx 10000))
                       (println "Finished line" idx "at"
                                (str (java.util.Date.)))))
                   (seque 30 (map-indexed vector adder-promises))))))
  (println "Done inserting for" datafile))


