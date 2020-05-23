*****************************************************************************************************************;
*                                                                                                               *;
*; %let pgm=cst_250;                                                                                            *;
*                                                                                                               *;
*  OPSYS  WIN 10 64bit SAS 9.4M6(64bit)  (This code will not run in lockdown)                                   *;
*                                                                                                               *;
*; %let purpose=Create the fact table for the snowflake schema;                                                 ;*;
*                                                                                                               *;
*  Github repository (sample input, output and code is at)                                                      *;
*                                                                                                               *;
*  https://github.com/rogerjdeangelis/CostReports                                                               *;
*                                                                                                               *;
*  PROJECT TOKEN = cst                                                                                          *;
*                                                                                                               *;
*  This is the fourth  module in the Cost Report Project                                                        *;
*                                                                                                               *;
*; %let _r=d; * home directory;                                                                                ;*;
*                                                                                                               *;
*;libname cst "&_r:/cst"; /* Where the schema tables and intermediary tables is stored */                       *;
*;libname  cstfmt "&_r:/cst/fmt";                                                                               *;
*                                                                                                               *;
*; options fmtsearch=(work.formats cst.cst_fmtv1a) sasautos="&_r:/oto" ;                                        *;
*                                                                                                               *;
*  worksheet descriptions                                                                                       *;
*  https://www.costreportdata.com/worksheet_formats.html                                                        *;
*                                                                                                               *;
* INTERNAL MACROS                                                                                               *;
* ===============                                                                                               *;
*                                                                                                               *;
*  cst_250 - first module in cost report analysis                                                               *;
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
*  Output from cst_200 macro                                                                                    *;
*                                                                                                               *;
*  &_r:\cst\CST_200SnfFiv20112019.sas7bdat                                                                      *;
*                                                                                                               *;
*  This is a external you need before rnning the code (this is outside the project)                             *;
*                                                                                                               *;
*  &_r:\cst\xls\cst_025snfrug.xlsx    * Descriptions for rug codes lost web link;                               *;
*                                                                                                               *;
*                                                                                                               *;
*  PROCESS                                                                                                      *;
*  =======                                                                                                      *;
*                                                                                                               *;
*    1. Check status of previous module, cst_200.                                                               *;
*                                                                                                               *;
*       If previous module executed without error then this table will exist                                    *;
*                                                                                                               *;
*       &_r:\cst\cst_200.sas7badat                                                                              *;
*                                                                                                               *;
*       else                                                                                                    *;
*                                                                                                               *;
*       it will not exist                                                                                       *;
*                                                                                                               *;
*       If it does not exist go into sytax check mode                                                           *;
*                                                                                                               *;
*                                                                                                               *;
*  OUTPUTS (works for all cost reports here is input for SNF)                                                   *;
*  =========================================================                                                    *;
*                                                                                                               *;
*  TBD                                                                                                          *;
*                                                                                                               *;
*                                                                                                               *;
*  Background Information                                                                                       *;
*                                                                                                               *;
*  WorkSheet                                                                                                    *;
*                                                                                                               *;
*  S1,S2, S3 CERTIFICATION AND SETTLEMENT SUMMARY                                                               *;
*                                                                                                               *;
*  S41       SNF-BASED HOME HEALTH AGENCY                                                                       *;
*                                                                                                               *;
*  O01       ANALYSIS OF HOSPITAL-BASED HOSPICE COSTS                                                           *;
*                                                                                                               *;
*  S7        PROSPECTIVE PAYMENT FOR SNF STATISTICAL DATA                                                       *;
*  A0        RECLASSIFICATION AND ADJUSTMENT OF TRIAL BALANCE OF EXPENSES                                       *;
*                                                                                                               *;
*  A7        RECONCILIATION OF CAPITAL COSTS CENTERS                                                            *;
*                                                                                                               *;
*  C0        COMPUTATION OF RATIO OF COSTS TO CHARGES                                                           *;
*  O0        ANALYSIS OF HOSPITAL-BASED HOSPICE COSTS                                                           *;
*                                                                                                               *;
*  E00A181   CALCULATION OF REIMBURSEMENT SETTLEMENT TITLE XVIII                                                *;
*  E00A192   ANALYSIS OF PAYMENTS TO PROVIDERS FOR SERVICES RENDERED                                            *;
*                                                                                                               *;
*  G0        BALANCE SHEET                                                                                      *;
*  G2        STATEMENT OF PATIENT REVENUES AND OPERATING EXPENSES                                               *;
*  G3        STATEMENT OF REVENUES AND EXPENSES                                                                 *;
*                                                                                                               *;
*****************************************************************************************************************;
*                                                                                                               *;
* CHANGE HISTORY                                                                                                *;
*                                                                                                               *;
*  1. Roger Deangelis              19JMAY2019   Creation                                                        *;
*     rogerjdeangelis@gamil.com                                                                                 *;
*                                                                                                               *;
*****************************************************************************************************************;


%macro cst_250(
     typ    = snf
    ,yrs    = 2011-2019
    ,outyrs = 2019
    ,inpsd1 = cst_200&typ.fiv
    ,coldes = cst_025&typ.describe
    ,outsd1 = cst_250&typ.fac
   ) / des="Create sas table and excel deliverables";

     /*
        %let typ    = snf;
        %let yrs    = 2011-2019;
        %let outyrs = 2019;
        %let inpsd1 = cst_200&typ.fiv;
        %let outsd1 = cst_250&typ.fac;
        %let coldes = cst_025&typ.describe;
     */

     %let yrscmp=%sysfunc(compress(&yrs,%str(-)));
     %put &yrscmp;

     %let inpSd1Fix=%sysfunc(compress(&inpsd1&yrscmp));
     %put &=inpsd1fix;

     %let outSd1fix=%sysfunc(compress(&outsd1&yrscmp));
     %put &=outsd1fix;

     %put &=outwkd;
/*
     proc datasets lib=cst;
        delete &outsd1fix;
     run;quit;

     proc datasets lib=work nolist mt=data mt=view;
        delete cst_250jynprp;
     run;quit;
*/
    * fact table have both _N and _C but describe table does not so generate both, only one will match;
    data cst_250jynprp;
        length col_cel_name $21;
        set cst.cst_025&typ.describe;
        col_cel_name =cats(col_cel_name,'_N') ; output ;
        col_cel_name =cats(col_cel_name,'_C') ; output ;
    run;quit;

    options compress=char;
    * join descriptions to fact table (all cells from csv);
    data cst.&outsd1fix (drop=_rc sortedby=yer wks);

      if _n_=1 then
        do;
          if 0 then set cst_250jynprp;
          dcl hash h(dataset:'cst_250jynprp');
          _rc=h.defineKey('col_cel_name');
          _rc=h.definedata('col_cel_description');
          _rc=h.defineDone();
        end;

      set cst.&inpsd1fix;

      if h.find(key:cstnam)=0 then output;
      else  do;col_cel_name="NOMATCH"; COL_CEL_DESCRIPTION="TBD"; output; end;

    run;quit;

     %if &syserr=0 %then %do;
        data cst.cst_200;
        run;quit;
     %end;

    /*
    NOTE: The data set CST.CST_250SNFFAC20112019 has 152634402 observations and 9 variables.
    NOTE: Compressing data set CST.CST_250SNFFAC20112019 decreased size by 69.90 percent.
          Compressed is 216703 pages; un-compressed would require 719974 pages.
    NOTE: DATA statement used (Total process time):
          real time           2:16.02
    */

%mend cst_250;

%cst_250(
     typ    = snf
    ,yrs    = 2011-2019
    ,inpsd1 = cst_200&typ.fiv
    ,coldes = cst_025&typ.describe
    ,outsd1 = cst_250&typ.fac
   ";
