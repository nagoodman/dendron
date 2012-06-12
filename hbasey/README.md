## Pre-requisites

Make sure you have installed lein. Get it here: https://github.com/technomancy/leiningen

Navigate to this folder with the shell and then enter `lein deps`. This will
download all dependencies. You may get a ZipException because apparently since
this project began one of the sub-dependencies is no longer available at its
old location. I included a copy of asm-3.1.jar in the repo, so if you get
the exception you have to install it to the local maven repo like so:

`mvn install:install-file -DgroupId=asm -DartificatId=asm -Dversion=3.1 -Dpackaging=jar -Dfile=/path/to/asm-3.1.jar`

To launch a repl manually, just run `lein repl`.

You can create an uber-jar that packages up the code and all the dependencies into one standalone
jar file.

`lein uberjar clojure.main`

If you have the jar already just run it like a normal jar file but with rlwrap.

rlwrap java -server -jar dendron-1.0.0-SNAPSHOT-standalone.jar

It will launch a repl. Once it has launched, run:

(use 'bi.gr8.cuber.core)
(ns bi.gr8.cuber.core)

this is equivalent to the `lein repl` environment.

## Purpose and Usage

This project encompasses two core functions related to Big Data Cubing.

1. Construction of the cube for later querying.
2. Querying the cube.

As a sub-function, the full environment's provided for debug and information gathering via the repl. Direct and indirect access to both HBase and DynamoDB is possible.

## Constructing the Cube

Presently we read in from CSV files and output to DynamoDB. Each row
must have the same number of fields, the dimensionality of the data represented
by how many fields there are (with the possibility of subtracting one field if the
final field is the value).

The final value of the CSV row will
be taken as the value to use in the cube construction if it is numeric.
If it is not numeric, the default is to use the value 1
and the data will be interpreted as counts and all fields contribute
to the dimensionality count.

Currently we set an upper limit of 10 on the dimensionality.

Before beginning make sure you place any necessary AWS credentials in "credentials.clj", there is a template included in this directory.

From the repl:

(construct-cube "cubename" "csvlocation")

This will create the DynamoDB table "cubename" with throughput provisions
{:read 100 :write 1000}.

45 seconds will be spent sleeping for the creation to complete.
If the table already exists, the function will continue on
and will not delete it. It will recreate the keys, you can skip this step
by binding *construct-keys?* to false.

To delete a table outside the AWS web console:

(clean-table (dyndb-table "cubename"))

If desired you can specify even more csv files during creation that will be unioned:

(construct-cube "cubename" "csvlocation" "csvlocation2" ...)

Note this currently requires two passes on the data. Significant speedups are
achievable if the CSV files are stored in /dev/shm. A future improvement
may cycle them through /dev/shm in the background before reading.
Each file is read sequentially, each row is kicked off using a future
to be processed in another thread.

## Querying the Cube

All queries return the aggregate from the start of the cube (i.e. the origin
[0 0 ... 0]) to the designated endpoint.

(query-cube "cubename" ["ending" "point" "cell"])

Each dimension's keys are ordered and map to a numeric. The numeric is determined by the sort-order of the unique keys seen in the initial CSV file(s).

If you already know the numerics for a particular end cell:

(query-cube "cubename" ^:numeric [0 1 2])

It's on the TODO to allow for more powerful/interesting queries.

## License

Copyright (C) 2012 DynamoBI

## Sanity Debugging

In tests.clj, running the code up until the second assert is done should
result in no assertion errors.

We may include another function that goes through every possible cell C
and verifies it satisfies the constraint where its value corresponds to
a sum from the original data set [L_1...L_d]:[C_1...C_d] inclusive, where
each L_i in L (the left-bound for the sum) is calculated by:

Let A be the anchor cell of the inner-most box containing C, which may be C
itself.
If A_i == C_i:
  L_i = 0
Else:
  L_i = A_i+1.

(Technically the else case implicitly satisfies the condition
A_i+1 <= C_i < A_i + k, with k=N/2, this may help calculate A if uncertain.)

For example, in a 3D 4x4x4 cube where each cell is counted once, N=4,
the value at cell C=[2 3 2] is 9.
The relevant anchor is A=[2 2 2], and by the formula above L=[0 3 0].
It is seen that [0 3 0]:[2 3 2] spans 9 cells. See
`(cube/cells-in-range [0 3 0] [0 3 2])`
