##for YEAR in 1987 1988 1989 1990 1991 1992 1993 1994 1995 1996 1997 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012; 
for YEAR in 2011 2010;
do
  echo Downloading for year $YEAR;
  for MONTH in 1 2 3 4 5 6 7 8 9 10 11 12; do
    echo ${YEAR}_$MONTH
    curl -O http://www.transtats.bts.gov/Download/On_Time_On_Time_Performance_${YEAR}_$MONTH.zip
  done
done
