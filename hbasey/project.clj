(defproject dendron "1.0.0-SNAPSHOT"
  :description "FIXME: write description"
  :main  bi.gr8.cuber.core
  :aot :all
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [clojure-hbase "0.90.5-3"]
                 [org.apache.hbase/hbase "0.92.1"]
                 [org.clojure/math.combinatorics "0.0.2"]
                 [com.amazonaws/aws-java-sdk "1.3.6"]
                 [rotary "0.2.3"]])
