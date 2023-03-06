proc product_status;
run;

proc options
     option=config;
run;
options obs=10;

data _null_;
   put "Fish report";
run;

/* Load data from fish.csv */
filename remotecvsdata s3 "/fisheries-catch-data/global-catch-data/csv/rfmo_12.csv" region="us-west-2";

/* Skip header, unwrap the double quote */
data fishes;
  infile remotecvsdata dlm="," firstobs=2 dsd;
  input rfmo_id rfmo_name$ layer_name$ year scientific_name$ common_name$ functional_group$ commercial_group$ fishing_entity$ sector_type$ catch_status$ reporting_status$ gear_name$ catch_sum$ real_value$;
run;

DATA filtered_fishes;
   SET fishes;
   DROP functional_group commercial_group fishing_entity sector_type;
RUN;

proc contents data=filtered_fishes;
run;

DATA cleaned_fishes;
   SET filtered_fishes;
   KEEP year scientific_name common_name;
RUN;

proc print data = cleaned_fishes width = full (obs=1);
    title 'Here are some fishes';
run;

proc means data = cleaned_fishes MIN MAX;
    title 'Stats';
    var year;
run;
