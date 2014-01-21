for x in `ls -1 On_Time*.csv`; 
do 
  echo $x; 
  cat $x | shuf -n -1000 >> sampled_1000_per_file.csv
done
