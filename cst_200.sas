*****************************************************************************************************************;
*                                                                                                               *;
*; %let pgm=cst_200;                                                                                            *;
*                                                                                                               *;
*  OPSYS  WIN 10 64bit SAS 9.4M6(64bit)  (This code will not run in lockdown)                                   *;
*                                                                                                               *;
*; %let purpose=Sum columns 1 thru 4 in worksheet G0;                                                          ;*;
*                                                                                                               *;
*  Github repository (sample input, output and code is at)                                                      *;
*                                                                                                               *;
*  https://github.com/rogerjdeangelis/CostReports                                                               *;
*                                                                                                               *;
*  PROJECT TOKEN = cst                                                                                          *;
*                                                                                                               *;
*  This is the third  module in the Cost Report Project                                                         *;
*                                                                                                               *;
*; %let _r=d; * home directory;                                                                                ;*;
*                                                                                                               *;
*;libname cst "&_r:/cst"; /* Where the schema tables and intermediary tables is stored */                       *;
*;libname  cstfmt "&_r:/cst/fmt";                                                                               *;
*                                                                                                               *;
*; options fmtsearch=(work.formats cst.cst_fmtv1a) sasautos="&_r:/oto" ;                                        *;
*                                                                                                               *;
*                                                                                                               *;
* INTERNAL MACROS                                                                                               *;
* ===============                                                                                               *;
*                                                                                                               *;
*  cst_200 - first module in cost report analysis                                                               *;
*                                                                                                               *;
* EXTERNAL MACROS IN AUTOCALL LIBRARY                                                                           *;
* ====================================                                                                          *;
*                                                                                                               *;
*  INPUTS (works for all cost reports here is input for SNF)                                                    *;
*  =========================================================                                                    *;
*                                                                                                               *;
*  The cost report token (example)                                                                              *;
*  The years to process must be sequential                                                                      *;
*                                                                                                               *;
*  typ    = snf                                                                                                 *;
*  yrs    = 2011-2019                                                                                           *;
*                                                                                                               *;
*  Output from cst_150 macro (2011-201 SNF all data from all CSVs(                                              *;
*                                                                                                               *;
*  &_r:\cst\CST_150SNFNUMALP20112019.sas7bdat                                                                   *;
*                                                                                                               *;
*  PROCESS                                                                                                      *;
*  =======                                                                                                      *;
*                                                                                                               *;
*    1. Check status of previous module, cst_150.                                                               *;
*                                                                                                               *;
*       If previous module executed without error then this table will exist                                    *;
*                                                                                                               *;
*       &_r:\cst\cst_150.sas7badat                                                                              *;
*                                                                                                               *;
*       else                                                                                                    *;
*                                                                                                               *;
*       it will not exist                                                                                       *
*                                                                                                               *;
*       If it does not exist go into sytax check mode                                                           *;
*                                                                                                               *;
*    2. Sum columns 1-4 in G0 worksheet and add create column 5                                                 *;
*                                                                                                               *;
*       example (creates a worksheet column 5 that does not exist in original work sheet)                       *;
*                                                                                                               *;
*        Worsheet Line                                                                                          *;
*                                                                                                               *;
*        G000000_01200_00500 =                                                                                  *;
*                                                                                                               *;
*         G000000_01200_00100 +                                                                                 *;
*         G000000_01200_00200 +                                                                                 *;
*         G000000_01200_00300 +                                                                                 *;
*         G000000_01200_00400                                                                                   *;
*                                                                                                               *;
*                                                                                                               *;
*  OUTPUTS (works for all cost reports here is input for SNF)                                                   *;
*  =========================================================                                                    *;
*                                                                                                               *;
*  1. Skinny fact table with addded column 5 in G0 worksheet                                                    *;
*                                                                                                               *;
*      &_r:\cst\CST_200SnfFiv20112019.sas7bdat                                                                  *;
*                                                                                                               *;
*  2. One record with the max value for every cell for all Years of SNF                                         *;
*                                                                                                               *;
*      &_r:\cst\CST_200SnfMax20112019.sas7bdat                                                                  *;
*                                                                                                               *;
*  3. If code ran sucessfully create sas table                                                                  *;
*                                                                                                               *;
*     &_r:\cst\CST_200.sas7bdat                                                                                 *;
*                                                                                                               *;
*                                                                                                               *;
*****************************************************************************************************************;
*                                                                                                               *;
* CHANGE HISTORY                                                                                                *;
*                                                                                                               *;
*  1. Roger Deangelis              19JMAY2019   Creation                                                        *;
*     rogerjdeangelis@gamil.com                                                                                 *;
*                                                                                                               *;
*****************************************************************************************************************;


%symdel typ yrs outfiv outmax inp;

%macro cst_200(
     typ    = snf
    ,yrs    = 2011-2019
    ,inp    = cst_150&typ.numalp
    ,outfiv = cst_200&typ.fiv
   ) / des="Create column five in G0 financial worksheet";

     /*
        %let typ    = snf;
        %let yrs    = 2011-2019;
        %let outfiv = cst_200&typ.fiv;
        %let inp    = cst_150&typ.numalp;
     */

     %let yrscmp=%sysfunc(compress(&yrs,%str(-)));
     %put &yrscmp;

     %let inpfix=%sysfunc(compress(&inp&yrscmp));
     %put &=inpfix;

     %let outfiv=%sysfunc(compress(&outfiv&yrscmp));
     %put &=outfiv;

     proc datasets lib=work nolist mt=data mt=view;
        delete cstvu &pgm.cstcsvsrt &pgm.sumcol;
     run;quit;

     proc datasets lib=cst nolist;
        delete &outfiv cst_200;
     run;quit;

     data cstvu/view=cstvu;
       length wkslynrec $20 csvdatkey $21;
       set cst.&inpfix (
         keep=yer rpt_rec_num prvdr_num WKSHT_CD CLMN_NUM LINE_NUM TYP itm_txt
         rename=itm_txt=csvdatval
       );
       csvdatkey=catx('_',strip(WKSHT_CD),strip(LINE_NUM),strip(CLMN_NUM),TYP);
       wkslynrec=cats(WKSHT_CD,LINE_NUM,rpt_rec_num);
       keep wkslynrec csvdatkey csvdatval rpt_rec_num prvdr_num yer;
     run;quit;

     * sort the view so G0 columns are together;
     proc sort data=cstvu out=&pgm.cstcsvsrt noequals sortsize=80g;
     by yer wkslynrec csvdatkey;
     run;quit;

     /*
     Up to 40 obs from CST_200CSTCSVSRT total obs=149,543,692

                                                        RPT_REC_                       PRVDR_
             WKSLYNREC               CSVDATKEY             NUM      YER    CSVDATVAL    NUM

        A000000001001000236    A000000_00100_00200_N     1000236    11     1344300     335257
        A000000001001000236    A000000_00100_00300_N     1000236    11     1344300     335257

     NOTE: PROCEDURE SORT used (Total process time):
     real time           3:35.17
     WORK.CST_200CSTCSVSRT has 149543692 observations and 6 variables
     */

     * add totals for G000000 workbook  index=(reckey=(rpt_rec_num cstnam)/unique;
     * unique to worksheet G000000;
     * sum the four columns in the csv file;
     data cst.&outfiv (sortedby=yer wks rename=(csvdatkey=cstnam csvdatval=cstval));
       retain typ "&typ"  amt 0 wks;
       length wks $7;
       set &pgm.cstcsvsrt;
       by yer wkslynrec csvdatkey;
       typ=upcase(typ);
       wks=substr(wkslynrec,1,7);
       if (substr(wkslynrec,1,7) ne 'G000000') then output;
       else do;
           select;
             when (first.wkslynrec and last.wkslynrec) do;
                   output;
                   substr(csvdatkey,17,1)='5';
                   output;
             end;
             when (first.wkslynrec ) do;output;amt=sum(input(csvdatval,?? best18.),0);end;
             when (last.wkslynrec  ) do;
                output;
                amt=sum(amt,input(csvdatval,?? best18.));
                substr(csvdatkey,17,1)='5';
                csvdatval=put(amt,best18. -l);
                output;
             end;
             otherwise do; amt=sum(amt,input(csvdatval,?? best18.)); output; end;
           end;
       end;
       drop amt wkslynrec;
     run;quit;

/*
     NOTE: DATA statement used (Total process time):
           real time           2:00.73


     proc sort data=&pgm.sumcol out=cst.&outfiv noequals force;
     by cstnam descending csvdatlen;
     run;quit;

     proc sort data=&pgm.sumcol out=cst.&outfiv noequals force;
     by cstnam descending csvdatlen;
     run;quit;

     data cst.&outmax;
        set cst.&outfiv;
        by cstnam descending csvdatlen;
        if first.cstnam;
        keep rpt_rec_num cstnam cstval;
     run;quit;
*/

     %if &syserr=0 %then %do;
        data cst.cst_200;
        run;quit;
     %end;

%mend cst_200;

options obs=max;
%cst_200(
     typ    = snf
    ,yrs    = 2011-2019
    ,inp    = cst_150&typ.numalp
    ,outfiv = cst_200&typ.fiv
   );
