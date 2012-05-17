(ns hbasey.newmodel)

(def sparse2d ^{:dimensions [[:year :qtr] :origin] :measure :qty}
  {[[2010 :q1] :ca] 2
   [[2010 :q1] :wa] 2
   [[2010 :q2] :ca] 5
   [[2010 :q2] :or] 4
   [[2010 :q2] :wa] 3
   [[2010 :q3] :ca] 2
   [[2010 :q4] :or] 2
   [[2011 :q1] :wa] 4
   [[2011 :q2] :ca] 2})

(def cube2d ^{:N 10}
  {[[2010 :q1] :ca] 2
   [[2010 :q1] :wa] 2
   [[2010 :q2] :ca] 5
   [[2010 :q2] :or] 4
   [[2010 :q2] :wa] 3
   [[2010 :q3] :ca] 7
   [[2010 :q4] :or] 6
   [[2010 :q4] :wa] 3
   [[2011 :q1] :wa] 4
   [[2011 :q2] :ca] 11
   ; these are necessary for queries to work accurately
   ; because their values are NOT the same as not-found=0
   [[2010 :q4] :ca] 7
   [[2011 :q1] :ca] 7
   ; and these are necessary for square queries along the 11-q2 dim
   [[2011 :q2] :or] 6
   [[2011 :q2] :wa] 15
   })

(def cube2d-meta
  ; dim-key [relative-key-anchor-at-top-level, next-level, ...]
  [[; first dimension
    [2010 :q1] [[2010 :q1]]
    [2010 :q2] [[2010 :q1], [2010 :q2]]
    [2010 :q3] [[2010 :q1], [2010 :q2], [2010 :q3]]
    [2010 :q4] [[2010 :q1], [2010 :q4]]
    [2011 :q1] [[2010 :q1], [2010 :q4], [2011 :q1]]
    [2011 :q2] [[2011 :q2]]
    ; four extra "unused"
    ]
   [
    ; second dimension
    :ca [:ca]
    :or [:ca :or]
    :wa [:ca :or :wa]
    ; 7 extra "unused"
    ]])

; part2 is obsolete
(def cube2d-meta-part2
  [
   [; first dimension (level 0 is the top, not the leaf)
    {:level 0 :anchor-pairs '[ ([2010 :q1] [2011 :q2]) ]}
    {:level 1 :anchor-pairs '[ ([2010 :q2] [2010 :q4])
                              (nil nil)]};(z w)
    ],
   [; second dimension
    {:level 0 :anchor-pairs '[ (:ca nil) ]} ; (:ca c)
    {:level 1 :anchor-pairs '[ (:or nil) ; (:or a)
                              (nil nil) ]} ;(d f)
    ]])

;(intern *ns* (symbol a) val)











(comment


(defn make-cube-iter [cube [cell data]]

  )

(loop [cube {} datas sparse2d]
  (if (seq datas)
    (recur (make-cube-iter cube (first datas)) (next datas))
   cube)) ; should output cube2d and cube2d-meta

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
      )))

)
