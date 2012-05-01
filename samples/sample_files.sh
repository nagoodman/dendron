for x in `ls -1 On_Time*.csv`; 
do 
  echo $x; 
  tail -1000 $x >> sampled_1000_per_file.csv
done
