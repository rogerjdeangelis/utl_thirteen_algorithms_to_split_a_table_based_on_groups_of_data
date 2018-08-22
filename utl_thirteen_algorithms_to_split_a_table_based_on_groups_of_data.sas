Thirteen algorithms to split a table based on groups of data;

see comments on end by
Paul Dorfman <sashole@bellsouth.net> comment on end

github
https://goo.gl/wD3EkA
https://github.com/rogerjdeangelis/utl_five_algorithms_to_split_a_table_based_on_a_categorical_variable

  Thirteen Splitting Solutions

     1. HASH without sort Paul Dorfman
     2. HASH without sort Low mamory Paul Dorfman
        (amazing only a hash and open=defer can create dynamic datasets)
        (hash should be able to create a defered new dataset within a dataset
         set x y open=defer where Yy is created by the hash)
     3. DOSUBL SQL then Datastep
     4. DOSUBL Sort and index then Datastep
     5. DOSUBL Just datastep  (very inefficient)
     6. R SPLIT function
     7. DOSUBL datastep inside proc sql (kind of useless)
     8. SQL array
     9. Datstep and macro (Chriss Hemedinger)
    10. DOSUBL in one SQL selct statement (Variation of 9)
    11. Call execute large dataset solution?
    12  Split datasst on columns
    13  Split dataset on rows


see
https://goo.gl/4QL1o7
https://communities.sas.com/t5/SAS-Statistical-Procedures/How-to-quot-Split-Data-quot-By-Group-Processing/m-p/431439

see
Novinosrin profile
https://communities.sas.com/t5/user/viewprofilepage/user-id/138205


INPUT
=====

 WORK.HAVE total obs=27                 |  RULES  (NW AND ow DATASETS)
                                        |
   WEIGHT    ID    TREATMENT    KCAL    |  WORK.NW total obs=15
                                        |
     NW      1         A         400    |    WEIGHT    ID    TREATMENT    KCAL
     NW      2         A         500    |
     OW      3         A         560    |      NW      1         A         400
     NW      4         A         800    |      NW      2         A         500
     OW      5         A         490    |      NW      4         A         800
     NW      6         A         500    |      NW      6         A         500
     OW      7         A         400    |    ...
                                        |  WORK.OW total obs=12
                                        |
                                        |    WEIGHT    ID    TREATMENT    KCAL
                                        |
                                        |      OW      3         A         560
                                        |      OW      5         A         490
                                        |      OW      7         A         400


PROCESS ( All the code)
=======================

 1. HASH without sort Paul Dorfman (this uses sashelp.citimon dataset)

    data _null_ ;
      dcl hash hh (ordered:"a") ;
      hh.definekey  ("m") ;
      hh.definedata ("mon", "h") ;
      hh.definedone () ;
      do until (z) ;
        set sashelp.citimon end = z ;
        m = month (date) ;
        mon = put (date, worddate3.) ;
        if hh.find() ne 0 then do ;
          dcl hash h (dataset:"sashelp.citimon(obs=0)", multidata:"y") ;
          h.definekey ("date") ;
          h.definedata (all:"y") ;
          h.definedone () ;
          hh.add() ;
        end ;
        h.add() ;
      end ;
      dcl hiter ihh ("hh") ;
      do while (ihh.next() = 0) ;
        h.output (dataset:mon) ;
      end ;
      stop ;
    run ;

    NOTE: The data set WORK.JAN has 13 observations and 19 variables.
    NOTE: The data set WORK.FEB has 12 observations and 19 variables.
    NOTE: The data set WORK.MAR has 12 observations and 19 variables.
    ...
    NOTE: The data set WORK.DEC has 12 observations and 19 variables.


 2. HASH without sort Low memory Paul Dorfman
 ============================================

    Interesting dynamic input and output datasets;

    1. It illustrates a curious angle of the dynamic nature of the hash object.
    2. I haven't seen it done this way before.

    data _null_ ;
      do i = 0 to 11 ;
        d = intnx("mon",0,i) ;
        dcl hash h (dataset:cats("sashelp.citimon(where=(month(date)=",month(d),"))"),multidata:"y") ;
        h.definekey ("date") ;
        h.definedata (all:"y") ;
        h.definedone() ;
        h.output (dataset:put(d,worddate3.)) ;
        h.clear() ;
      end ;
      stop ;
      set sashelp.citimon ;
    run ;


 3. DOSUBL SQL then Datastep
 ===========================

    data _null_;

      * faster than distinct;
      if _n_=0 then do;
        %let rc=%sysfunc(dosubl('
          proc sql;
             select quote(max(weight)) into :wgts separated by "," from have  group by weight
          ;quit;
        '));
      end;

      do wgt=&wgts;
        call symputx("wgt",wgt);
        rc=dosubl('
          data &wgt;
            set have(where=(weight=symget("wgt")));
          run;quit;
        ');
      end;

    run;quit;


 4. SORT DOSUBL (need an index on weight)
 =======================================

    data _null_;

      if _n_=0 then do;
        %let rc=%sysfunc(dosubl('
          proc sort data=have out=havSrt(index=(weight));
             by weight;
          run;quit;
        '));
      end;

      set havSrt(keep=weight);
      by weight;
      if last.weight then do;
         call symputx('wgt',weight);
         rc=dosubl('
            data &wgt;
               set have(where=(weight=symget("wgt")));
            run;quit;
        ');
      end;
    run;quit;


 5. DOSUBL JUST DATASTEP
 =======================

    * just in case they exist;
    proc datasets lib=work;
      delete ow nw rec;
    run;quit;

    data _null_;

      set have(keep=weight);
      call symputx('wgt',weight);
      call symputx('rec',_n_);

      * need to optimizing compiler;
      rc=dosubl('
        data rec; set have; if _n_=&rec then do; output;stop;end; run;quit;
        proc append base=&wgt data=rec;
        run;quit;
      ');

    run;quit;


 6. R SPLIT FUNCTION
 ===================

    %utl_submit_wps64('
    libname sd1 sas7bdat "d:/sd1";
    options set=R_HOME "C:/Program Files/R/R-3.3.2";
    libname wrk sas7bdat "%sysfunc(pathname(work))";
    proc r;
    submit;
    source("C:/Program Files/R/R-3.3.2/etc/Rprofile.site", echo=T);
    library(haven);
    have<-read_sas("d:/sd1/have.sas7bdat");
    want<-split(have, list(have$WEIGHT), drop = TRUE);
    list2env(want,envir=.GlobalEnv);
    endsubmit;
    import r=OW  data=wrk.wantwpsow;
    import r=NW  data=wrk.wantwpsnw;
    run;quit;
    ');

    proc print data=wantwpsnw(obs=4);
    run;quit;

    Up to 40 obs from wantwpsnw total obs=15

    Obs    WEIGHT    ID    TREATMENT    KCAL

      1      NW      1         A         400
      2      NW      2         A         500
      3      NW      4         A         800
      4      NW      6         A         500


  7. DOSUBL datastep inside proc sql
  ==================================


     * I keep an emty dataset around because sql requires on even when you don't need one;

     data sasuser.empty;
       rc=0;
     run;quit;

     %symdel NW OW weights  / nowarn; * just in case you rerun;
     proc sql;
        select
           max(weight) into :weights separated by " "
        from
           have
        group
           by weight
        ;
        select
           dosubl('
            data &weights;
               set have;
               select (weight);
                 %array(wgts,values=&weights)
                 %do_over(wgts,phrase=%str(when ("?") output ?;))
               end;
            run;quit;') as rc
        from
           sasuser.empty
     ;quit;


  8. SQL Array
  ==================================


     * I keep an empty dataset around because sql requires on even when you don't need one;

     data sasuser.empty;
       rc=0;
     run;quit;

     %symdel NW OW weights  / nowarn; * just in case you rerun;
     proc sql;
        select
           max(weight) into :weights separated by " "
        from
           have
        group
           by weight
        ;
        %array(wgts,values=&weights)
        %do_over(wgts,phrase=%str(
           proc sql;
            create table ? as select *
            from have where weight="?";quit;))
     ;quit;


  9. Datastep and macro  Chris Hemedinger
  ========================================

     Chris Hemedinger

     %let TABLE=sashelp.cars;
     %let COLUMN=origin;

     proc sql noprint;
     /* build a mini program for each value */
     /* create a table with valid chars from data value */
     select distinct
        cat("DATA out_",compress(&COLUMN.,,'kad'),
        "; set &TABLE.(where=(&COLUMN.='", &COLUMN.,
        "')); run;") into :allsteps separated by ';'
       from &TABLE.;
     quit;

     /* macro that includes the program we just generated */
     %macro runSteps;
      &allsteps.;
     %mend;

     /* and...run the macro when ready */
     %runSteps;


  10. DOSUBL in one SQL selct statement
  =====================================

     proc sql noprint;

       /* build a mini program for each value */
       /* create a table with valid chars from data value */

       select
          distinct
            dosubl(catx(" ",
              "data",sex
                ,";set sashelp.class(where=(sex=",quote(sex)
                ,"));run;quit;"
          ))
       from
          sashelp.class

     ;quit;




        cat("DATA out_",compress(&COLUMN.,,'kad'),
        "; set &TABLE.(where=(&COLUMN.='", &COLUMN.,
        "')); run;") into :allsteps separated by ';'
       from &TABLE.;
     quit;

     /* macro that includes the program we just generated */
     %macro runSteps;
      &allsteps.;
     %mend;

     /* and...run the macro when ready */
     %runSteps;

  11. Call execute large dataset solution?

      * put the levels of sex into &sec = "F" "M";
      proc sql;
        select distinct quote(strip(sex)) into :sex separated by ' ' from sashelp.class
      ;quit;

      %put &=sex;    /* the levels "F" "M";
      %put &=sqlobs; /* then number of levels */

      data _null_;
         length sas $200;
         array arysex[&sqlobs] $32 (&sex);
         sas='data ';
         call execute(sas);

         do i=1 to &sqlobs;
            sas=arysex[i];
            call execute(sas);
         end;

         /* change dataset and sex for other data */
         sas=';set sashelp.class; select (sex);';
         call execute(sas);

         do i=1 to &sqlobs;
            sas=catx(' ',cats("when ('",arysex[i],"')"),'output',arysex[i],';');
            call execute(sas);
            put sas;
         end;

         sas='otherwise;end;run;quit;';
         call execute(sas);
      run;quit;


/* T3101140 Split a dataset by sets of variables using SQL

Same results in WPS and SAS

github
https://github.com/rogerjdeangelis/utl_split_a_dataset_by_sets_of_variables_using_sql

see for do_over macro
https://goo.gl/EUYyaB
https://github.com/rogerjdeangelis/utl_sql_looping_or_using_arrays_in_sql_do_over_macro/blob/master/utl_sql_looping_or_using_arrays_in_sql_do_over.sas


stackoverflow
https://stackoverflow.com/questions/48969348/sas-proc-sql-select-columns-belonging-together

INPUT
=====

   RULES (USE SQL and create two datasets C1 and C2

    1.  Create dataset C1 with varaibles _C10_--_C19_
    2.  Create dataset C2 with varaibles _C20_--_C29_


 WORK.HAVE  total obs=1

 Obs _C10_ _C11_ _C12_ _C13_ _C14_ _C15_ _C16_ _C17_ _C18_ _C19_

  1  37851 52717  6400 35160 29928  8803 45861 11191 34847 13532

 Obs _C20_ _C21_ _C22_ _C23_  _C24_ _C25_ _C26_ _C27_ _C28_ _C29_

  1  42999 38913  5202 32790 100150 35176 43656 61507  9979 37850


PROCESS
=======

  proc sql;
     create
        table c1 as
     select
        %array(Cs,values=1-9)
        %do_over(cs,phrase=_C1?_,between=comma)
     from
        have
     ;
     create
        table c2 as
     select
        %do_over(cs,phrase=_C2?_,between=comma)
     from
        have
     ;
  quit;


OUTPUT
======

 WORK.C1 total obs=1

  Obs _C11_ _C12_ _C13_ _C14_ _C15_ _C16_ _C17_ _C18_ _C19_

   1  52717  6400 35160 29928  8803 45861 11191 34847 13532


 WORK.C1 total obs=1

  Obs _C21_ _C22_ _C23_  _C24_ _C25_ _C26_ _C27_ _C28_ _C29_

   1  38913  5202 32790 100150 35176 43656 61507  9979 37850

*                _              _       _
 _ __ ___   __ _| | _____    __| | __ _| |_ __ _
| '_ ` _ \ / _` | |/ / _ \  / _` |/ _` | __/ _` |
| | | | | | (_| |   <  __/ | (_| | (_| | || (_| |
|_| |_| |_|\__,_|_|\_\___|  \__,_|\__,_|\__\__,_|

;

proc report data=sashelp.cars nowd missing out=have(keep=_C10_--_C29_);
cols make, weight;
define make / across;
define weight / sum;
run;quit;

*          _       _   _
 ___  ___ | |_   _| |_(_) ___  _ __
/ __|/ _ \| | | | | __| |/ _ \| '_ \
\__ \ (_) | | |_| | |_| | (_) | | | |
|___/\___/|_|\__,_|\__|_|\___/|_| |_|

;


* SAS;
proc sql;
   create
      table c1 as
   select
      %array(Cs,values=1-9)
      %do_over(cs,phrase=_C1?_,between=comma)
   from
      have
   ;
   create
      table c2 as
   select
      %do_over(cs,phrase=_C2?_,between=comma)
   from
      have
   ;
quit;


* WPS;
%utl_submit_wps64('
libname wrk "%sysfunc(pathname(work))";

 proc sql;
   create
      table c1 as
   select
      %array(Cs,values=1-9)
      %do_over(cs,phrase=_C1?_,between=comma)
   from
      wrk.have
   ;
   create
      table c2 as
   select
      %do_over(cs,phrase=_C2?_,between=comma)
   from
      wrk.have
   ;
 quit;

');

/* T3099810 StackOverflow SAS: Split row into two columns with sas proc transpose

github
https://gist.github.com/rogerjdeangelis/b13b651116304c000749b2fca9bba2ab

https://goo.gl/rj7Diz
https://stackoverflow.com/questions/47849771/split-row-into-two-columns-with-sas-proc-transpose



 WORK.HAVE total obs=3                             |      RULES
                                                   |
   UNIT    A1     B1     A2     B2     A3     B3   | unit  rows  A     B
                                                   |
    1      A11    B11    A21    B21    A31    B31  |   1    1    A11   B11  for unit=1 create 3 rows 2 cols
    2      A12    B12    A22    B22    A32    B32  |   1    2    A21   B21
    3      A13    B13    A23    B23    A33    B33  |   1    3    A31   B31
                                                   |
                                                   |   2    1    A12   B12
                                                   |   2    2    A22   B22
                                                   |   2    3    A32   B32
 PROCESS
========

  proc transpose data=have out=havxpo;
     var a1-a3 b1-b3;
     by unit;
  run;quit;

  data want;
    * get the dimsnsion of the array ;
    if _n_=0 then do;
      %let rc=dosubl('
          proc sql;
            select max(substr(_name_,2)) into :dim trimmed
            from havxpo
          ;quit;
      ');
    end;

    retain unit row a b x1-x%eval(&dim.*2);
    array mat3x2[3,2] $3 x1-x%eval(&dim.*2);
    * load 3x2 array, temporary would be better;
    do row=1 by 1 until(last.unit);
      set havXpo;
      by unit;
      select ;
        when ( row <= 3 ) mat3x2[row,1]=col1;
        otherwise         mat3x2[row-3,2]=col1;
      end;
    end;
    * output array 2 columns at a time;
    do row=1 to 3;
         a=mat3x2[row,1];
         b=mat3x2[row,2];
         keep unit row a b;
         output;
    end;
  run;quit;

OUTPUT
======

  WORK.WANT total obs=9

     UNIT    ROW     A      B

      1       1     A11    B11
      1       2     A21    B21
      1       3     A31    B31

      2       1     A12    B12
      2       2     A22    B22
      2       3     A32    B32

      3       1     A13    B13
      3       2     A23    B23
      3       3     A33    B33

*                _              _       _
 _ __ ___   __ _| | _____    __| | __ _| |_ __ _
| '_ ` _ \ / _` | |/ / _ \  / _` |/ _` | __/ _` |
| | | | | | (_| |   <  __/ | (_| | (_| | || (_| |
|_| |_| |_|\__,_|_|\_\___|  \__,_|\__,_|\__\__,_|

;
data have;
   input (unit  A1  B1  A2  B2  A3  B3) ($);
cards4;
 1 A11 B11 A21 B21 A31 B31
 2 A12 B12 A22 B22 A32 B32
 3 A13 B13 A23 B23 A33 B33
;;;;
run;quit;

proc transpose data=have out=havxpo;
   var a1-a3 b1-b3;
   by unit;
run;quit;

/*
Up to 40 obs WORK.HAVXPO total obs=18

    UNIT    _NAME_    COL1

     1        A1      A11
     1        A2      A21
     1        A3      A31
     1        B1      B11
     1        B2      B21
     1        B3      B31
     2        A1      A12
     2        A2      A22
     2        A3      A32
     2        B1      B12
     2        B2      B22
     2        B3      B32
     3        A1      A13
     3        A2      A23
     3        A3      A33
     3        B1      B13
     3        B2      B23
     3        B3      B33
*/

data havRow;
  if _n_=0 then do;
    %let rc=dosubl('
        proc sql;
          select max(substr(_name_,2)) into :dim trimmed
          from havxpo
        ;quit;
    ');
  end;
  retain unit row a b x1-x%eval(&dim.*2);
  array mat3x2[3,2] $3 x1-x%eval(&dim.*2);
  do row=1 by 1 until(last.unit);
    set havXpo;
    by unit;
    select ;
      when ( row <= 3 ) mat3x2[row,1]=col1;
      otherwise         mat3x2[row-3,2]=col1;
    end;
  end;
  do row=1 to 3;
       a=mat3x2[row,1];
       b=mat3x2[row,2];
       keep unit row a b;
       output;
  end;
run;quit;


unit  rows  A     B
  1    1    A11   B11
  1    2    A21   B21
  1    3    A31   B31
  2    1    A12   B12
  2    2    A22   B22
  2    3    A32   B32

*____             _
|  _ \ __ _ _   _| |
| |_) / _` | | | | |
|  __/ (_| | |_| | |
|_|   \__,_|\__,_|_|

;


Roger,

A very long in the tooth "problem". IIRC correctly,
I saw a question like this on SAS-L first circa 1997.

Interestingly, the "solution" chosen by the OP is the worst possible since it reads the
input as many times are distinct cities plus one for the sorting.

Incidentally, my "HASH without sort Low memory" would essentially do the same. I offered
it in that 'sashelp.citimon" split thread because of its apparent oddity - kind of "never done before" thing.

The question is, what is the best approach overall. Weeding out those that have to reread
the input data set more than once, I see only these as viable:

1. By reading the input once, generate code for the DATA statement and an IF-THEN-ELSE of
SELECT structure. Then read the input for the second time and get all you need. The method
of generating code doesn't matter, whether it's a macro, CALL EXECUTE, DOSUB, PUT/%include, etc.
Advantage: Does not need any sorting.
Disadvantage: With too many output data sets (a few hundred, say), it's easy to get the
memory inundated by the buffers of all the output data set opened at compile since,
unlike with the SET statement, their opening cannot be deferred.

2. Sort and hash one BY group at a time using the OUTPUT method after each BY group.
Advantage: Only one output data set is opened at a time.
Disadvantage: Needing to sort. If the BY groups are more or less equal, memory usage is not an issue.

3. Same as #2 but index rather than sort. With a short key and lots of distinct values,
may be optimal. If one BY group is huge relative to the rest, may have a memory issue.

4. Hash of hashes.
Advantage: No sorting, a single pass through the input file. From the geek factor
 POV, the most aesthetically pleasing of all.
Disadvantage: Memory. Need to be able to stick the entire input data set in there.

As always in computing: Give some, take some.

Best regards


