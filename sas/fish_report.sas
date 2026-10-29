/* Fish catch report: reads a CSV of the project, cleans it, summarises it and writes an HTML
   report and a CSV export. Runs with graal-sas, the SAS language interpreter of the graal Data
   Platform (no SAS licence needed). Paths are relative to the project root; outputs go to the
   run's output directory. */

/* Parameter: the first year to report on, from the job's environment (default 2018). */
%let min_year = 2018;
%if %sysfunc(envlen(MIN_YEAR)) > 0 %then %let min_year = %sysget(MIN_YEAR);
%put NOTE: Reporting on catches from &min_year;

options nocenter;
title 'Fish catch report';

/* Read the CSV: skip the header line, comma-separated, quoted values allowed. */
data fishes;
  length region $ 20 scientific_name $ 40 common_name $ 30 functional_group $ 20 sector_type $ 12;
  infile 'data/catch.csv' dlm=',' dsd firstobs=2 truncover;
  input year region $ scientific_name $ common_name $ functional_group $ sector_type $
        catch_tonnes landed_value_usd;
run;

proc contents data=fishes varnum;
run;

/* Keep the recent years and the columns the report needs. */
data cleaned_fishes;
  set fishes;
  where year >= &min_year;
  value_per_tonne = round(landed_value_usd / catch_tonnes, 0.01);
  format landed_value_usd comma14. value_per_tonne comma10.2;
  drop functional_group sector_type;
run;

proc print data=cleaned_fishes(obs=5) noobs;
  title2 'First observations';
run;

proc means data=cleaned_fishes min max mean maxdec=1;
  title2 'Catch statistics (tonnes)';
  class common_name;
  var catch_tonnes;
run;

proc sql;
  create table catch_by_year as
  select year, region, sum(catch_tonnes) as total_tonnes format=12.1,
         count(*) as species
  from cleaned_fishes
  group by year, region
  order by year, region;
quit;

/* An HTML report and a CSV export, written to the run's output directory. */
ods html file='fish_report.html' title='Fish catch report';
proc print data=catch_by_year noobs label;
  title2 'Catch by year and region';
  label total_tonnes = 'Total catch (tonnes)' species = 'Species';
  format total_tonnes comma12.1;
run;
proc sgplot data=catch_by_year;
  vbar year / response=total_tonnes stat=sum group=region groupdisplay=stack;
  yaxis label='Catch (tonnes)' grid;
run;
ods html close;

proc export data=catch_by_year outfile='catch_by_year.csv' dbms=csv replace;
run;
