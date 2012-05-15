(ns hbasey.dcubeutils
  (:require [clojure.math.combinatorics :as combin]))

;summing op

(def op +)
(def arc-op -)

(defn keydist
  "Given 2 keys in the same dimension, how 'far' apart are they?"
  [key1 key2]
  (Math/abs (double (- key1 key2))))

(defn keydiff [key1 key2]
  (- key1 key2))

(defn keysum [key1 key2]
  (+ key1 key2))


(defn anchor-slots [origin N] ; size 2^dim of origin
  (apply combin/cartesian-product (map #(range %1 (+ %1 N) (/ N 2)) origin)))
(def anchor-slots (memoize anchor-slots))

(defn gen-centers [anchors length]
  (map (fn [anchor] (map #(+ length %1) anchor)) anchors))
(def gen-centers (memoize gen-centers))

(defn shared-anchor [anchors bordercell]
  ; find the anchor such that only one dimension key is different
  ; from the border cell, and for the dimension key k that is different,
  ; select the anchor such that bordercell@k - anchor@k > 0 and is the smallest
  ; (there will always be only 2 possible anchors to pick from)
  (let [[[anchor dim] [anchor2 _]]
          (keep (fn [anchor]
                  (let [neq-dims (keep-indexed
                                   (fn [dim itm]
                                     (if (not= itm (nth bordercell dim)) dim))
                                   anchor)]
                    (if (= 1 (count neq-dims)) [anchor (first neq-dims)])))
                anchors)
        borderd (bordercell dim)
        anchord1 (nth anchor dim)
        anchord2 (nth anchor2 dim)
        diff1 (keydiff borderd anchord1)
        diff2 (keydiff borderd anchord2)]
    (or (and (neg? diff1) anchor2)
        (and (neg? diff2) anchor)
        (and (> diff1 diff2) anchor2)
        (and (> diff2 diff1) anchor))))

(defn intersecting-borders [cell anch]
  ; for each dimension of cell, start from the anchor and travel
  ; in the current dimension until the keys are the same, and take this
  ; cell.
  (let [anchor (vec anch)]
    (keep-indexed (fn [idx dim]
                    (let [oldval (nth anchor idx)]
                      (assoc anchor idx (keysum oldval (keydiff dim oldval)))))
                  cell)))

; currently does not work in 3D...
; the "not-any" is too restrictive. perhaps it's better phrased
; as "no more than d-1"?
(defn where-is-cell "http://www.youtube.com/watch?v=JwRzi-E1l40" [cell origin N]
  (let [anchors (anchor-slots origin N)
        length (/ N 4)
        in-hypercube? (fn [center] ; returns the given center point of
                        ; a hypercube if the a cell is within its boundaries
                        ; (assuming its boundaries are N/4 in length in each
                        ; dimension)
                        (let [diffs (map keydist cell center)]
                          (if (not-any? #(>= %1 length) diffs)
                            ; this is the center that this cell belongs in
                            center
                            ; else we're in a border-cell
                            false)))]
    (if (some #(>= %1 N) (map keydiff cell origin)) :outside
      (if (not= -1 (.indexOf anchors cell))
        :anchor
        ; check quadrants in parallel, if nil, it's a border cell
        (if-let [center-q (some identity (pmap in-hypercube?
                                               (gen-centers anchors length)))]
          center-q
          :border)))))
(def where-is-cell (memoize where-is-cell))


(defn query-ddc-range "returns ranged agg from origin to cell"
  [cube origin cell]
  (let [N ((meta cube) :N)
        op ((meta cube) :op)
        inner-box-length (/ N 4) ; used to adjust result of 'location' when it's "inside" to get the proper anchor
        location (where-is-cell cell origin N)
        ]
    (println "cell with respect to " origin " is at " location)
    (cond
      (= location :outside) 0
      (= location :anchor) ; just value at cell
        (cube cell)
      (= location :border) ; value at cell op'd with value at anchor
        (op (cube cell) (cube (shared-anchor (anchor-slots origin N) cell)))
      :else ; need to recursively descend with anchor as new origin
        ; after op'ing the anchor cell + dimension-matching borders
        (let [anchor (map int (map keydiff location (repeat inner-box-length)))
              borders (intersecting-borders cell anchor)
              border-vals (apply op (pmap #(cube %1) borders))]
          (op (cube anchor) border-vals (query-ddc-range (with-meta cube (assoc (meta cube) :N (dec (/ N 2)))) (map keysum anchor (repeat 1)) cell))))))


(defmulti parse-ddc-instructions (fn [cube cell instructions]
                                   (some #{:border-of :anchor-of :parent-anchor}
                                         (flatten (vec instructions)))))

(defn query-ddc-range-fixed "does not rely on key order"
  [cube cell]
  (let [op (or ((meta cube) :op) +)
        [value instructions] (cube cell)]
    (loop [result value
           [newval newinst] (parse-ddc-instructions cube cell instructions)]
      (if (= {} newinst)
        value
        (recur (op value newval) (parse-ddc-instructions cube cell newinst))))))

(defmethod parse-ddc-instructions :border-of [cube cell instructions]
  ; need anchor value we're a border of
  (cube (instructions :border-of)))

(defmethod parse-ddc-instructions :anchor-of [cube cell instructions]
  ; need the value of the border cells sum
  )

(defmethod parse-ddc-instructions :parent-anchor [cube cell instructions]
  ; need in

(defmethod parse-ddc-instructions :default [_ _ _] [0 {}]) ; no further cells

; this fn still needs work
(defn insert-into-cube
  "Build cube iteratively, returns new cube."
  [cube origin [cell value]]
  (println (meta cube))
  (let [N ((meta cube) :N)
        location (where-is-cell cell origin N)]
    (cond
      (= location :outside)
        (let [[newloc newn] (loop [loc location n N]
                              (println "looping.. " loc n)
                              (if-let [newn (and
                                              (= loc :outside)
                                              (* 2 (inc n)))]
                                (recur (where-is-cell cell
                                                      origin newn)
                                       newn)
                                [loc n]))
              newcube (with-meta cube (assoc (meta cube) :N newn))]
          ; we "grew" cube to fit the data. We have to insert&fill in the border
          ; cells and anchor cell(s) to their appropriate values
          ; (assuming for v1 that we grew append-only)
          (cond
            (= newloc :anchor)
              (assoc newcube cell (op value (newcube origin)))
            (= newloc :border)
              (let [newanchor (shared-anchor (anchor-slots origin newn) cell)]
                (assoc newcube newanchor (newcube origin) cell (op value))
            )
          ))
      (= location :border) 0; only need to update border cells that
        ; include this one and exist after in any dimension until anchor is hit.
        ; that is, 
      (= location :anchor) ; anchors are the sum from origin to anchor
        ; of original data, but cube is ddc.
        ; Anchors that already exist in the cube just update with an op.
        (if (= cell origin)
          (do (println "here") (assoc cube cell value))
          (assoc cube cell (op value (cube cell))))
      ;
      ; anchors at the origin are
        ; themselves. Anchors not at the origin are computed by
        ; value + 3.
      ))))


(defn sort-cube [cube] ; obsolete
  (sort
    (proxy [java.util.Comparator] []
      (compare [o1 o2]
        (compare (first o1) (first o2))))
    (partition 2 cube)))

(defn cube2csv [cube] ;2d only of course
  (let [sorted-cube (sort cube) ;(sort-cube cube)
        [[N _] _] (last sorted-cube)]
    (loop [rows (partition (inc N) sorted-cube)]
      (when-first [row rows]
        (println
          (apply str (drop-last (apply str (map #(str (second %1) ",") row)))))
        (recur (next rows))))))

