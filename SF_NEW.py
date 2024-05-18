import numpy as np
import pandas as pd
import snowflake.connector
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import datetime
from dateutil.parser import parse
import multiprocessing as mp
from multiprocessing import Pool
from functools import partial

from pympler import asizeof
from pympler import tracker
from pympler import summary
from snowflake.connector import ProgrammingError

import string
import sys
import re

try:


       stg_db=sys.argv[1]
       stg_schema=sys.argv[2]
       job_name=sys.argv[3]
       entity_id=sys.argv[4]
       vParameter_fld=sys.argv[5]
       vStatus_flg=sys.argv[6]
       vENTITY_ATTRIBUTES=pd.DataFrame()




       vEntity_ID=sys.argv[1]
       vRule_Type=sys.argv[2]
       vUser=  sys.argv[3]
       vPassword= sys.argv[4]
       vAccount=sys.argv[5]
       vDB_NAME= sys.argv[6]
       vSTG_DB_NAME=sys.argv[7]
       vWH_Name= sys.argv[8]
       vJob_Instance_Id = sys.argv[9]
       vTotal_Count = 0
       vSource_System='Default'
       vPROCESS_CONTROL='PROCESS_CONTROL_DEV'

       vENTITY_ATTRIBUTES=pd.DataFrame()  ##defining empty variable is useful to in case process goes into finally block before creating it otherwise it throws NameError


       con = snowflake.connector.connect(
        user=vUser,
        password=vPassword,
        account=vAccount,
        paramstyle='qmark',
                warehouse= vWH_Name,
                database=vSTG_DB_NAME

        )
        
       engine = create_engine(
                'snowflake://{user}:{password}@{account}/{database_name}/{schema_name}?warehouse={warehouse_name}'.format(
                        user= vUser,
                        password= vPassword,
                        account=vAccount,
                        database_name=vSTG_DB_NAME,
                        schema_name='STG',
                        warehouse_name=vWH_Name,
                        pool_size=20,
                        autocommit=True,
                        reset_on_return='commit'
        )
        )
                
       def logProcessAudit(request_id, vCHECKPOINT_LABEL, vCHECKPOINT_DESC , vSQL_TEXT):
                vLog_SQL="INSERT INTO "+Temp_Error_Log+" ( REQUEST_ID ,  CHECKPOINT_LABEL,     CHECKPOINT_DESC , SQL_TEXT) VALUES ("+request_id+",'"+ vCHECKPOINT_LABEL +"','"+vCHECKPOINT_DESC+"','"+vSQL_TEXT+"')"
                con.cursor().execute(vLog_SQL)
        
       def drop_table(p_db_name,p_schema_name,p_tbl_name):
            vDrop_SQL="DROP TABLE IF EXISTS "+p_db_name+"."+p_schema_name+"."+p_tbl_name+";"
            con.cursor().execute(vDrop_SQL)
                
       def check_sql_injection(sql_text):
            inject_flag = 1  # Assume no SQL injection by default
            sql_injection_keywords = [' DELETE ', ' INSERT ', ' UPDATE ', ' ALTER ', ' DROP ']
    
            for keyword in sql_injection_keywords:
                if keyword.lower() in sql_text.lower():
                    inject_flag = 0
                    break
    
            return inject_flag
            
       def replace_string(input_str, replace_pattern, replace_with):
            if replace_pattern not in input_str or replace_pattern == replace_with:
                return input_str
            
            replaced_str = input_str.replace(replace_pattern, replace_with)
            return replaced_str
        
       def execute_sql(sql_text, request_id):
            
            # Initialize the status flag
            status_flag = 0
            
            try:
                # Execute the provided SQL text
                con.cursor().execute(sql_text)
                
            except ProgrammingError as e:
                # If an exception occurs, set the status flag to 1 and log the error
                status_flag = 1
                error_code = e.errno
                error_text = e.msg
                
                # Log the error to your check_error_log table
                con.cursor.execute("INSERT INTO" +Temp_Error_Log+ "(request_id, checkpoint_label, checkpoint_desc) VALUES (%s, %s, %s)", (request_id, error_code, f"{error_text}: {sql_text}"))
                
                
            # Return the status flag
            return status_flag
        
       def execute_sql_and_check_result(sql):
            cursor = con.cursor()
            # Execute the provided SQL
            cursor.execute(sql)
            query_id = cursor.sfqid
            # Fetch results using RESULT_SCAN
            cursor.execute(f"SELECT * FROM TABLE(RESULT_SCAN('{query_id}'))")
            sql_command = cursor.fetchall()

            # Check if all values in the first row are 0
            if not sql_command:
                return 0  # Return 0 if the result set is empty
            elif all(value == 0 for value in sql_command[0]):
                return 0  # Return 0 if condition is met
            else:
                return 1  # Return 1 otherwise
                
       def execute_sql_combined(sql_text, request_id=None):
            # Initialize the status flag and result flag
            status_flag = 0
            result_flag = 1  # Default to 1, indicating condition not met or there's an error

            try:
                # Execute the provided SQL text
                cursor = con.cursor()
                cursor.execute(sql_text)
                query_id = cursor.sfqid  # Store the query ID for RESULT_SCAN

                # No exception means SQL executed successfully, now check the result
                cursor.execute(f"SELECT * FROM TABLE(RESULT_SCAN('{query_id}'))")
                results = cursor.fetchall()

                # Determine the result_flag based on the condition
                if not results:
                    result_flag = 0  # Result set is empty
                elif all(value == 0 for value in results[0]):
                    result_flag = 0  # All values in the first row are 0
                # If neither condition is met, result_flag remains 1

            except snowflake.connector.ProgrammingError as e:
                # If an exception occurs, set the status flag to 1 and log the error
                status_flag = 1
                error_code = e.errno
                error_text = e.msg

                if request_id is not None:
                    # Log the error to your check_error_log table, assuming `con.cursor().execute` for insertion
                    cursor.execute("INSERT INTO "+Temp_Error_Log+" (request_id, checkpoint_label, checkpoint_desc)VALUES (%s, %s, %s)", (request_id, error_code, f"{error_text}: {sql_text}"))

            #finally:
                #cursor.close()  # Ensure the cursor is closed after operation

            # Return both the status flag and result flag
            return status_flag, result_flag
         




       '''
REPLACE PROCEDURE PROCESS_CONTROL.sp_error_check

 (

  IN stg_db VARCHAR(30), 
  IN job_name VARCHAR(50),
  IN entity_id INTEGER,
  IN parameter_fld VARCHAR(50) ,
  OUT status_flg INTEGER
 )

 BEGIN 
 ## ==============================================================================================
 ## DECLARE local variables
 ## ==============================================================================================
 
 DECLARE entity_name       VARCHAR(30);
 DECLARE entity_id_tt     INTEGER;
 DECLARE select_criteria VARCHAR(400);
 DECLARE select_criteria_temp VARCHAR(500);#added as part of REQ0103614 ## to store select criteria query at run time */
 DECLARE LOB VARCHAR(100); #added as part of REQ0103614 ## to store derived LOB  */
 DECLARE error_log_tbl VARCHAR(20); #added as part of REQ0103614  ## to store error log table specific to LOB */
 DECLARE entity_active_ind VARCHAR(1); #added as part of REQ0103614 - to store entity active indiactor value*/
 DECLARE reprocess_ind	INTEGER;#added as part of REQ0103614 ## to store reprcoeesable rule count */
 DECLARE sc_where VARCHAR(10); 
 DECLARE staging_Ind VARCHAR(1);
 DECLARE error_row_table      VARCHAR(30);
 DECLARE type_ind         BYTEINT; 
 DECLARE source_record_pk    VARCHAR (4000);
 DECLARE concat_source_record_pk  VARCHAR (4000);
 DECLARE concat_source_record_pk_1  VARCHAR (4000);
 DECLARE column_list        VARCHAR (12000);  #modify column length */
 DECLARE column_list_err     VARCHAR (12000);#modify column length */
 DECLARE job_id          INTEGER;
 DECLARE total_records       INTEGER;
 DECLARE schm_db         VARCHAR (30);
 DECLARE err_schm_db         VARCHAR (30);
 DECLARE request_id        INTEGER;
 DECLARE good_records       INTEGER;
 DECLARE threshold_rejection     INTEGER;
 DECLARE v_return_message2     VARCHAR (1000);
 DECLARE modified_by        VARCHAR (100);
 DECLARE modified_dt        VARCHAR (32);
 DECLARE Notify_flg         INTEGER;
 DECLARE tt_tbl           VARCHAR(30);
 DECLARE  tt_err_tbl          VARCHAR(30);
 DECLARE tt_err_re_tbl        VARCHAR(30);
 DECLARE tt_err_log_tbl        VARCHAR(30);
 DECLARE tt_err_log_act_ind_tbl        VARCHAR(30);#added as part of REQ0103614 ## to store error log data for inactive error ids */
 DECLARE tt_log_tbl         VARCHAR(30);
 DECLARE created_dt TIMESTAMP(6);
 DECLARE sql_stmt         VARCHAR(3000);
 DECLARE primary_index varchar(2000);
 DECLARE databasename varchar(2000);
 DECLARE tablename varchar(2000);
 
 DECLARE sql_text         VARCHAR(32000) ;
 DECLARE sql_text_temp       VARCHAR(32000) ;
 DECLARE sql_text_exec       VARCHAR(32000) ;#added as part of REQ0103614 ## to store query executed at run time in the procedure*/ 
 DECLARE active_passive_flag    CHAR(1) ;
 ## Ideally rule attribute should be of varchar 30
 DECLARE rule_attribute1       VARCHAR(1000) ;
 DECLARE rule_attribute2       VARCHAR(1000) ;
 DECLARE rule_attribute3       VARCHAR(1000) ;
 DECLARE rule_attribute4       VARCHAR(1000) ;
 DECLARE rule_attribute5       VARCHAR(1000) ;
 DECLARE record_id         DECIMAL(22,0);
 DECLARE Inject_flag        INTEGER;
  
 DECLARE dq_metric_id      INTEGER;
 DECLARE fail_count        INTEGER;
 DECLARE success_count     INTEGER;
 DECLARE rule_id          INTEGER;
 DECLARE actual_value       DECIMAL(18,4);
 
 DECLARE error_text         VARCHAR(2000);
 DECLARE error_code       VARCHAR(2000);
 DECLARE status_flag        INTEGER;
 
 DECLARE Primary_Ind_Val         VARCHAR(2000);
 
 
 DECLARE v_stmt_total_rec     VARCHAR (2000) DEFAULT '';
 DECLARE v_stmt_dq_actuals    VARCHAR (2000) DEFAULT '';
 DECLARE v_first_column_name  VARCHAR (2000);#Created variable to store first column from attribute_list and reference the same in ROW_NUMBER() function*/

 DECLARE c_total_records     CURSOR FOR stmt_total_rec;
 DECLARE c_dq_actuals      CURSOR FOR stmt_dq_actuals;

       '''

       status_flg = 100
 ## ==============================================================================================
 ## step 1.4 Get Job_id AND request_id
 ## ==============================================================================================
 
 
       job_id = pd.read_sql_query("SELECT   job_id, MAX(created_date) created_date FROM "+vPROCESS_CONTROL+".CORE.dashboard_job_checks WHERE  job_name = "+job_name+" GROUP BY 1", engine)
       job_id=job_id['job_id'][0]
       created_date=job_id['created_date'][0]
  
## ==============================================================================================
## step 1.2  Get Entity attributes 
## Added ACTIVE_IND as part of REQ0103614 to skip exception process
## ==============================================================================================
 
 
       entity = pd.read_sql_query("SELECT   entity_name, entity_id,  error_row_table_name ,  type_ind, UPPER(select_criteria) as select_criteria, staging_ind, source_record_id, attribute_list, threshold_rejection, schema_description, error_row_table_schema, active_ind FROM  "+vPROCESS_CONTROL+".CORE.entity WHERE entity_id ="+entity_id, engine)
       entity_name=entity['entity_name'][0]
       entity_id_tt=entity['entity_id'][0]
       error_row_table=entity['error_row_table_name'][0]
       type_ind=entity['type_ind'][0]
       select_criteria=entity['select_criteria'][0]
       staging_ind=entity['staging_ind'][0]
       source_record_pk=entity['source_record_id'][0]
       column_list=entity['attribute_list'][0]
       threshold_rejection=entity['threshold_rejection'][0]
       schm_db=entity['schema_description'][0]
       err_schm_db=entity['error_row_table_schema'][0]
       entity_active_ind=entity['active_ind'][0]
 
 
 
  ## ==============================================================================================
 ## step 1.13  Get LOB  and error log table name for Entity as part of REQ0103614 Modified_Dt :08-MAR-2017 |
 ## 																															Modified_By : Amit
 ## ==============================================================================================
 
       error_log_tbl = pd.read_sql_query("SELECT 'error_log_' ||trim(lob) as error_log_tbl FROM "+vPROCESS_CONTROL+".CORE.SUBJECT_AREA_SOURCE_SYSTEM WHERE SUBJECT_SOURCE_ID IN ( SELECT SUBJECT_SOURCE_ID FROM "+vPROCESS_CONTROL+".CORE.entity where entity_id =" +entity_id_tt, engine)
       error_log_tbl=error_log_tbl['error_log_tbl'][0]
 
 
 
## ==================================================================================================================
## Modified_Dt :19-JAN-2016 |Modified_By : Harneet :## Added the below section to capture the unique column value as Primary index Value, 
## 																				to assign with the same index value to the temporary table 
  ## ==================================================================================================================
       Primary_Ind_Val = source_record_pk
 ## ==============================================================================================
 ##   Capture the User AND current_time
 ## ==============================================================================================
 #INTO :modified_by ,:modified_dt*/
       modified = pd.read_sql_query("SELECT CURRENT_USER AS user,CAST (CURRENT_TIMESTAMP(6) AS VARCHAR(32)) AS modified_dt",engine)
       modified_by=modified['user'][0]
       modified_dt=modified['modified_dt'][0]


 
 ## ==============================================================================================
 ## step 1.3  Concatenate the Primary Key fields
 ## ==============================================================================================
 
 
   # CALL PROCESS_CONTROL.replace_string ( source_record_pk , ',',  '~', concat_source_record_pk_1 , v_return_message2 );
  
   # CALL PROCESS_CONTROL.replace_string (  'COALESCE(cast('||concat_source_record_pk_1||' as varchar(4000)),''/NULL'')'  ,  '~',  ' as varchar(4000)),''/NULL'') ||''|''  || COALESCE(cast(' ,concat_source_record_pk , v_return_message2 );
   
   # concat_source_record_pk_1 = replace_string(source_record_pk, ',', '~')
   # concat_source_record_pk = replace_string('COALESCE(cast(' + concat_source_record_pk_1 + ' as varchar(4000)),\'/NULL\')', '~', ' as varchar(4000)),\'/NULL\') ||\'|\' || COALESCE(cast(')

   
 
   # source_record_pk = concat_source_record_pk
 
 ## ==============================================================================================
 ## step 1.4 Get Job_id and request_id
 ## ==============================================================================================
 #:request_id
       request_id = pd.read_sql_query("SELECT request_id FROM (SELECT request_id, RANK () OVER ( PARTITION BY job_id ORDER BY Last_Updated_Date DESC ) Ranking FROM "+vPROCESS_CONTROL+".CORE.dashboard_job_checks WHERE job_name = "+job_name+" AND created_date = created_dt  QUALIFY Ranking=1) REQ",engine)
       request_id = request_id['request_id'][0]

## ==============================================================================================
 ## step - Create error log table
 ## Modified statement as part of REQ0103614 Modified_Dt :08-MAR-2017 | to insert sql executed in check_error_log
 ## Modified_By : Amit
 ## ==============================================================================================

       tt_log_tbl = pd.read_sql_query("SELECT SUBSTR( 'tt_log_tbl'|| CAST ("+request_id+" AS VARCHAR(10)) || CAST ("+job_id+" AS VARCHAR(10))||CAST ("+entity_id_tt+" AS VARCHAR(10)),1,30) as tt_log_tbl",engine)
       tt_log_tbl= tt_log_tbl['tt_log_tbl'][0]
       Temp_Error_Log = stg_db+'.'+stg_schema+'.'+tt_log_tbl
       vExecSQL='CREATE or replace TABLE '+Temp_Error_Log+'  LIKE '+vPROCESS_CONTROL+'.check_error_log;'
       con.cursor().execute(vExecSQL)

       sql_text_exec = 'SELECT request_id INTO '+request_id+' FROM ( SELECT request_id, RANK () OVER ( PARTITION BY job_id ORDER BY Last_Updated_Date DESC ) Ranking FROM '+vPROCESS_CONTROL+'.CORE.dashboard_job_checks  WHERE job_name = '+job_name+' AND created_date = created_dt  QUALIFY Ranking=1 ) REQ | CREATE MULTISET TABLE '+stg_db+ '.' +tt_log_tbl+'  LIKE '+vPROCESS_CONTROL+'.check_error_log ;'   
## added as part of REQ0103614to capture query executed at this step*/

       vLog_Message='Request Id captured'
       logProcessAudit(request_id,'1 ',vLog_Message,sql_text_exec)
              
              
       if entity_active_ind == 'Y': 
 
 ## ==============================================================================================
 ##  step 1.5   Create and populate temporary tales
 ## ==============================================================================================
 
 ##LABEL_CREATE_TEMP :BEGIN
 
## tt_tbl =  SUBSTR( 'tt_'||CAST (job_id AS VARCHAR(10))||'_'||CAST (entity_id_tt AS VARCHAR(10)),1,30);
       tt_tbl = pd.read_sql_query("SELECT SUBSTR( 'tt'||CAST (request_id AS VARCHAR(10))||CAST (job_id AS VARCHAR(10))||CAST (entity_id_tt AS VARCHAR(10)),1,30) as tt_tbl",engine)
       tt_tbl=tt_tbl['tt_tbl'][0]

        ## tt_err_tbl  = SUBSTR( 'tt_err_'||CAST (job_id AS VARCHAR(10))||'_'||CAST (entity_id_tt AS VARCHAR(10)),1,30);
       tt_err_tbl = pd.read_sql_query("SELECT SUBSTR( 'tt_err'||CAST (request_id AS VARCHAR(10))||CAST (job_id AS VARCHAR(10))||CAST (entity_id_tt AS VARCHAR(10)),1,30) as tt_err_tbl",engine)
       tt_err_tbl=tt_err_tbl['tt_err_tbl'][0]
        
        #tt_err_re_tbl =   SUBSTR( 'tt_err_re_'||CAST (job_id AS VARCHAR(10))||'_'||CAST (entity_id_tt AS VARCHAR(10)),1,30);
       tt_err_re_tbl = pd.read_sql_query("SELECT SUBSTR( 'tt_err_re'||CAST (request_id AS VARCHAR(10))||CAST (job_id AS VARCHAR(10))||CAST (entity_id_tt AS VARCHAR(10)),1,30) as tt_err_re_tbl",engine)
       tt_err_re_tbl=tt_err_re_tbl['tt_err_re_tbl'][0]
        
        ## tt_err_log_tbl =  SUBSTR( 'tt_errlg_'||CAST (job_id AS VARCHAR(10))||'_'||CAST (entity_id_tt AS VARCHAR(10)),1,30);
       tt_err_log_tbl =pd.read_sql_query("SELECT SUBSTR( 'tt_errlg'||CAST (request_id AS VARCHAR(10))||CAST (job_id AS VARCHAR(10))||CAST (entity_id_tt AS VARCHAR(10)),1,30) as tt_err_log_tbl",engine)
       tt_err_log_tbl=tt_err_log_tbl['tt_err_log_tbl'][0]
        
       tt_err_log_act_ind_tbl = pd.read_sql_query("SELECT SUBSTR( 'tt_inact'||CAST (request_id AS VARCHAR(10))||CAST (job_id AS VARCHAR(10))||CAST (entity_id_tt AS VARCHAR(10)),1,30) as tt_err_log_act_ind_tbl",engine) 
       tt_err_log_act_ind_tbl=tt_err_log_act_ind_tbl['tt_err_log_act_ind_tbl'][0]
        
        #added as part of REQ0103614 to store error log data which are no longer active*/
        
       if select_criteria is None or select_criteria.empty:
            sc_where = ''
            select_criteria = ''
        
       else:
        
            #sc_where = pd.read_sql_query("SELECT CASE WHEN SUBSTRING(('WHERE '), 1, 6) <> 'WHERE ' THEN 'WHERE' ELSE '' END AS sc_where;"),engine)
            #sc_where=sc_where['sc_where'][0]


            sc_where = "WHERE" if select_criteria.strip().upper().startswith("WHERE") else ""


# Fetch the result using pandas
            #df = pd.read_sql_query(sql_query, conn)

        
       drop_table(stg_db,stg_schema,tt_tbl)

	##Modified_Dt :19-JAN-2016 |Modified_By :Harneet ## Added the Primary_Ind_Value as the Primary Index of the temporary table
	
       vExecSQL=f'CREATE TABLE {stg_db}.{stg_schema}.{tt_tbl} AS (SELECT * FROM (SELECT * FROM {stg_db}.{stg_schema}.{entity_name}) temp);'
       vExecSQL1=vExecSQL
       con.cursor().execute(vExecSQL)
 
 
       vExecSQL=f'ALTER TABLE {stg_db}.{stg_schema}.{tt_tbl} ADD error_record CHAR(1), ADD record_id DECIMAL(22,0), ADD error_create_date TIMESTAMP(6);'
       vExecSQL2=vExecSQL
       con.cursor().execute(vExecSQL)
 
		   
 #sql_text_exec = '''CREATE MULTISET TABLE ' ||   stg_db|| '.' || tt_tbl  ||' as  (SELECT * FROM (SELECT* FROM   '|| schm_db  +'.'||  entity_name  || ') temp)  WITH NO DATA  PRIMARY INDEX ( ' || Primary_Ind_Val||  ') | ALTER TABLE '||  stg_db|| '.' || tt_tbl  || ' ADD error_record CHAR(1) ,  ADD record_id DECIMAL(22,0),   ADD error_create_date TIMESTAMP(6) ''' ; ##added as part of REQ0103614 ## to capture query executed at this step */
       sql_text_exec = vExecSQL1+vExecSQL2
 
       vLog_Message='Temporary staging table has been created'
       logProcessAudit(request_id,'2',vLog_Message,sql_text_exec)

       v_first_column_name = pd.read_sql_query("select CASE WHEN POSITION(',',attribute_list)= 0 THEN '1' ELSE SPLIT_PART(attribute_list, ',', 1) END AS AttList FROM "+vPROCESS_CONTROL+".CORE.entity WHERE entity_id ="+entity_id,engine);# Version 0004 Changes */
       v_first_column_name=v_first_column_name['AttList'][0]
 
 
 
       # vExecSQL='INSERT INTO '+stg_db+'.'+stg_schema+'.'+tt_tbl+' (  '+column_list+', error_record, record_id ) SELECT  '+column_list+',''N'+',ROW_NUMBER() over (ORDER BY  ' +v_first_column_name+' ) as record_id FROM '+schm_db +'.' +entity_name || ' STG' ||' '|| sc_where||' '|| select_criteria''''''

       vExecSQL = 'INSERT INTO '+stg_db+'.'+stg_schema+'.'+tt_tbl+' (' +column_list + ', error_record, record_id) SELECT ' + column_list + ', \'N\', ROW_NUMBER() OVER (ORDER BY ' + v_first_column_name + ') AS record_id FROM ' + schm_db + '.' + entity_name + ' STG ' + sc_where + ' ' + select_criteria



       con.cursor().execute(vExecSQL)
 
 
 
       select_criteria_temp = pd.read_sql_query("select replace(select_criteria,'''','''''') as select_criteria",engine) 
       select_criteria_temp = select_criteria_temp['select_criteria'][0]
       ## added as part of REQ0103614 to capture query executed at this step*/
  
  
       vLog_Message='Temporary staging table has been populated'
       logProcessAudit(request_id,'3',vLog_Message,sql_text_exec)

			

            
 ## ==============================================================================================
 ##  step 1.9 ## Calculate Total Records in the Entity
 ## ==============================================================================================
 

  ## ==============================================================================================
  ## Forming runtime SELECT statement to count records
  ## ==============================================================================================
       sql_text_exec="SELECT count(1) total_records FROM "+stg_db+"."+stg_schema+"."+tt_tbl+" WHERE ERROR_RECORD = 'N';"
       v_stmt_total_rec = pd.read_sql_query(sql_text_exec,engine)
       total_records=v_stmt_total_rec['total_records'][0]
       vLog_Message='Count of records in the staging table has been captured'
       logProcessAudit(request_id,'4',vLog_Message,sql_text_exec)

  ## ==============================================================================================
  ## Dynamic cursor based on above dynamic statement.
  ## ==============================================================================================

       

  # PREPARE stmt_total_rec FROM v_stmt_total_rec;
  # OPEN c_total_records;
  
  # FETCH c_total_records INTO total_records;
  
       sql_text_exec = '''SELECT count(1) total_records FROM ' ||  stg_db|| '.' || tt_tbl   || ' WHERE ERROR_RECORD = ''''N'''' ''' ;#added as part of REQ0103614 ## to capture query executed at this step */
  
       
  
                    
 ## ==============================================================================================
 ##  step 1.6 - DELETE Record FROM Error Record IF most recent update has arrived
 ## As part of REQ0103614 capture error ids which are delete from error table
 ## ==============================================================================================
 
       drop_table(stg_db,stg_schema, tt_err_log_act_ind_tbl);	 
 
       vExecSQL='CREATE OR REPLACE TABLE '+stg_db+'.'+stg_schema+ '.'+tt_err_log_act_ind_tbl+'  AS  ( SELECT * FROM '+vPROCESS_CONTROL+'.CORE.'+error_log_tbl+');'
       con.cursor().execute(vExecSQL)
        #added as part of REQ0103614 ## temporary table for error log*/	 
		   
       sql_text_exec = '''CALL PROCESS_CONTROL.DROP_TABLE ( '||stg_db||', '||tt_err_log_act_ind_tbl||') | CREATE MULTISET TABLE ' || stg_db|| '.' || tt_err_log_act_ind_tbl  ||' AS (SELECT * FROM PROCESS_CONTROL.'|| error_log_tbl ||')  WITH NO DATA  PRIMARY INDEX ( ERROR_ID) ''' ;#added as part of REQ0103614 ## to capture query executed at this step*/

  	
       vLog_Message='Temporary error table for inactive error ids has been created'
       logProcessAudit(request_id,'5',vLog_Message,sql_text_exec)

       if vParameter_fld is None:
 
              sql_text_exec='INSERT INTO '+stg_db+'.'+stg_schema+'.'+tt_err_log_act_ind_tbl+'(error_id,request_id,rule_id,source_record_id,job_id,failed_record_attrib01,failed_record_attrib02 ,failed_record_attrib03,failed_record_attrib04,failed_record_attrib05,created_by,creation_date,last_updated_by,last_updated_date ,error_create_date ,reprocessed_record, active_ind) SELECT  error_id, request_id, rule_id, source_record_id, job_id, failed_record_attrib01,failed_record_attrib02,failed_record_attrib03,failed_record_attrib04,failed_record_attrib05, created_by, creation_date,    last_updated_by,last_updated_date ,error_create_date ,reprocessed_record, \'N\' as active_ind   FROM '+vPROCESS_CONTROL+'.CORE.'+error_log_tbl+' WHERE ERROR_ID  in( SELECT ERROR_ID  FROM '+err_schm_db+'.'+error_row_table+' WHERE ('+source_record_pk+')  in ( SELECT  ' +source_record_pk+' FROM '+stg_db+'.'+stg_schema+ '.' +tt_tbl+' GROUP BY 1));'
              con.cursor().execute(sql_text_exec)
               ##added as part of REQ0103614 ## to capture inactive error ids in temporary error log table*/	
       
       #  sql_text_exec = '''INSERT INTO ' ||   stg_db|| '.' || tt_err_log_act_ind_tbl  || ' (error_id, request_id, rule_id, source_record_id,    job_id, failed_record_attrib01,      failed_record_attrib02 , failed_record_attrib03 ,  failed_record_attrib04 , failed_record_attrib05, created_by, creation_date,    last_updated_by,      last_updated_date ,  error_create_date ,reprocessed_record, active_ind)  SELECT  error_id,      request_id,       rule_id,        source_record_id,    job_id,  failed_record_attrib01,      failed_record_attrib02 ,      failed_record_attrib03 ,  failed_record_attrib04 ,  failed_record_attrib05, created_by, creation_date,    last_updated_by,      last_updated_date , error_create_date ,reprocessed_record, ''''N'''' as active_ind   FROM '+vPROCESS_CONTROL+'.CORE.'||error_log_tbl||' WHERE ERROR_ID  in ( SELECT ERROR_ID  FROM '|| err_schm_db || '.' || error_row_table||'  WHERE ('|| Primary_Ind_Val || ')  in ( SELECT ' || Primary_Ind_Val|| ' FROM ' ||   stg_db|| '.' || tt_tbl  || ' GROUP BY ' || Primary_Ind_Val|| '))''' ;#added as part of REQ0103614 ## to capture query executed at this step
       

              vLog_Message='Temporary error table has been populated for inactive error ids'
              logProcessAudit(request_id,'6',vLog_Message,sql_text_exec)

              vExecSQL='DELETE  FROM '+err_schm_db+'.'+error_row_table+' WHERE ('+source_record_pk+')  in ( SELECT '+source_record_pk+ ' FROM '+stg_db+'.'+stg_schema+ '.' +tt_tbl+' GROUP BY 1);'
              con.cursor().execute(vExecSQL)              
			 
              sql_text_exec = 'DELETE  FROM '+err_schm_db+'.'+error_row_table+' WHERE ('+source_record_pk+')  in ( SELECT '+source_record_pk+ ' FROM '+stg_db+'.'+stg_schema+ '.' +tt_tbl+' GROUP BY 1);'#added as part of REQ0103614 ## to capture query executed at this step */

 
       else:
 
        vExecSQL='INSERT INTO '+stg_db+'.'+stg_schema+ '.' +tt_err_log_act_ind_tbl+'  (error_id,      request_id,       rule_id,        source_record_id,    job_id, failed_record_attrib01,      failed_record_attrib02 ,      failed_record_attrib03 ,  failed_record_attrib04 ,               failed_record_attrib05, created_by, creation_date,    last_updated_by,      last_updated_date , error_create_date ,reprocessed_record, active_ind)  SELECT  error_id,      request_id,       rule_id,        source_record_id,    job_id,               failed_record_attrib01,      failed_record_attrib02 ,      failed_record_attrib03 ,  failed_record_attrib04 ,               failed_record_attrib05, created_by, creation_date,    last_updated_by,      last_updated_date ,               error_create_date ,reprocessed_record, \'N\' as active_ind   FROM '+vPROCESS_CONTROL+'.CORE.'+error_log_tbl+' WHERE ERROR_ID in (                 SELECT ERROR_ID  FROM '+err_schm_db+ '.' +error_row_table+' WHERE ('+source_record_pk+ ',' +vParameter_fld+') in (                 SELECT ' +source_record_pk+  ',' +vParameter_fld+' FROM '+stg_db+'.'+stg_schema+ '.' +tt_tbl+' GROUP BY 1,2 ))'
        con.cursor().execute(vExecSQL)
                #added as part of REQ0103614 ## to capture inactive error ids in temporary error log table*/	
 
        sql_text_exec = '''INSERT INTO ' ||   stg_db|| '.' || tt_err_log_act_ind_tbl  || ' (error_id, request_id, rule_id, source_record_id,    job_id, failed_record_attrib01,      failed_record_attrib02 , failed_record_attrib03 ,  failed_record_attrib04 , failed_record_attrib05, created_by, creation_date,    last_updated_by,      last_updated_date ,  error_create_date ,reprocessed_record, active_ind)  SELECT  error_id,      request_id,       rule_id,        source_record_id,    job_id,  failed_record_attrib01,      failed_record_attrib02 ,      failed_record_attrib03 ,  failed_record_attrib04 ,  failed_record_attrib05, created_by, creation_date,    last_updated_by,      last_updated_date , error_create_date ,reprocessed_record, ''''N'''' as active_ind   FROM '+vPROCESS_CONTROL+'.CORE.'||error_log_tbl||' WHERE ERROR_ID  in ( SELECT ERROR_ID  FROM '|| err_schm_db || '.' || error_row_table||'  WHERE ('|| Primary_Ind_Val || ',' || parameter_fld ||' )  in ( SELECT ' || Primary_Ind_Val|| ',' || parameter_fld ||' FROM ' ||   stg_db|| '.' || tt_tbl  || ' GROUP BY ' || Primary_Ind_Val||  ',' || parameter_fld || '))''' ;#added as part of REQ0103614 ## to capture query executed at this step*/

        vLog_Message='Temporary error table has been populated for inactive error ids'
        logProcessAudit(request_id,'6',vLog_Message,sql_text_exec)

        vExecSQL='DELETE  FROM '+err_schm_db+'.' +error_row_table+' WHERE ('+source_record_pk+ ',' +vParameter_fld+') in ( SELECT ' +source_record_pk+  ',' +vParameter_fld+' FROM '+stg_db+'.'+stg_schema+ '.' +tt_tbl+' GROUP BY 1,2 )'
        con.cursor().execute(vExecSQL)
 
       #  sql_text_exec = '''DELETE  FROM ' ||   err_schm_db|| '.' || error_row_table  ||' WHERE (  ' || Primary_Ind_Val || ',' || parameter_fld ||' )  in ( SELECT '  || Primary_Ind_Val|| ',' || parameter_fld ||' FROM '|| stg_db +'.' || tt_tbl || ' GROUP BY ' || Primary_Ind_Val||  ',' || parameter_fld || ' )   ''' ;#added as part of REQ0103614 ## to capture query executed at this step*/
 
        #END IF;  
 
 
        vLog_Message='Records have been deleted from error row table for which most recent update has arrived'
        logProcessAudit(request_id,'7',vLog_Message,vExecSQL)


 
 ## ==============================================================================================
 ##  step 1.11   Create temporary error table 
 ## ==============================================================================================

        drop_table(stg_db,stg_schema,tt_err_tbl)
 
 ##-----Modifcation:Added the Primary_Ind_Value as the Primary Index of the temporary table
 
        vExecSQL='CREATE  TABLE '+stg_db+'.'+stg_schema+ '.' +tt_err_tbl+' AS (SELECT * FROM (SELECT* FROM  '+schm_db +'.'+entity_name+ ') temp); '
        con.cursor().execute(vExecSQL)
 
        vExecSQL='ALTER TABLE '+stg_db+'.'+stg_schema+ '.' +tt_err_tbl+' ADD active_passive_flag CHAR(1),   ADD rule_id INTEGER, ADD record_id DECIMAL(22,0),    ADD  error_create_date TIMESTAMP(6);'
        con.cursor().execute(vExecSQL)
 
        sql_text_exec = ''' CALL PROCESS_CONTROL.DROP_TABLE ( '||stg_db||','||tt_err_tbl||'  ) | CREATE MULTISET TABLE ' || stg_db|| '.' || tt_err_tbl  ||' AS (SELECT * FROM (SELECT* FROM  '|| schm_db +'.'||  entity_name  || ') temp)  WITH NO DATA  PRIMARY INDEX ( ' || Primary_Ind_Val||  ') | ALTER TABLE '|| stg_db|| '.' || tt_err_tbl  || '  ADD active_passive_flag CHAR(1),   ADD rule_id INTEGER,  ADD record_id DECIMAL(22,0),    ADD  error_create_date TIMESTAMP(6) ''';##added as part of REQ0103614 ## to capture query executed at this step*/

 
        vLog_Message='Temporary  error table has been created'
        logProcessAudit(request_id,'8',vLog_Message,sql_text_exec)
 
 
 ## ==============================================================================================
 ##  step 1.7 - Fetch the Re-process Flag for all the Error Records applicable for this run
 ## ==============================================================================================
 
        drop_table(stg_db,stg_schema, tt_err_re_tbl  )
 

        vExecSQL='CREATE TABLE '+stg_db+'.'+stg_schema+ '.' +tt_err_re_tbl+'  LIKE '+stg_db+'.'+stg_schema+ '.' +tt_err_tbl+';'
        con.cursor().execute(vExecSQL)
		   

        sql_text_exec = 'DROP_TABLE ( '+stg_db+','+tt_err_re_tbl+'  ) | CREATE TABLE ' + stg_db+ '.' + tt_err_re_tbl  +' LIKE  '+ stg_db +'.'+stg_schema+ '.'+  tt_err_tbl  +';'
        #added as part of REQ0103614 ## to capture query executed at this step*/
  
  
        vLog_Message='Temporary error reprocess table has been created'
        logProcessAudit(request_id,'9',vLog_Message,sql_text_exec)
        column_list_Err = pd.read_sql_query("SELECT 'ERR.' || replace( attribute_list,',' ,',ERR.') as column_list_Err FROM  "+vPROCESS_CONTROL+".CORE.entity WHERE entity_id = "+entity_id+";",engine)
        column_list_Err=column_list_Err[column_list_Err][0]
 
 
 ##--Modified_Dt :08-MAR-2017 |Modified_By : Amit|Desc :All rules repcrecoessing indicator are 'N', skip reprocessing--
 
 
        reprocess_ind = 0

        reprocess_ind=pd.read_sql_query("SELECT count(1) as reprocess_ind FROM  "+vPROCESS_CONTROL+".CORE.BUSINESS_RULE WHERE entity_id = "+entity_id+"  AND job_id = "+job_id+" AND REPROCESS_FLAG = 'Y' AND ENABLED_FLAG = 'Y';",engine)
        reprocess_ind=reprocess_ind[reprocess_ind][0]
 
       if reprocess_ind >= 1:

 
              if vParameter_fld is None:
 
 ##--Modified_Dt : 31-Aug-2015 |  Modified_By : Annu | Desc : In Select Query chaned Stg_db to err_schm_db
 ##--Modified_Dt :19-JAN-2016 |Modified_By : NTiwari:group by :In Select Query to avoid duplicate issue--
 ##--Modified_Dt : 22-JAN-2018 |Modified_By : AKumar18|Desc : Restricted aged based exception to be reprocessed on certain days
 
                     vExecSQL='INSERT INTO '    +stg_db+'.'+stg_schema+ '.' +tt_err_re_tbl+ ' ( ' +column_list+' ,record_id ,error_create_date)  SELECT '+column_list_Err+ ' ,Err.error_id,Err_LOG.error_create_date               FROM '+err_schm_db  +'.'+error_row_table + ' Err ,'+vPROCESS_CONTROL+'.CORE.'+error_log_tbl+ ' as Err_LOG ,'+vPROCESS_CONTROL+'.CORE.business_rule  RULE , '+vPROCESS_CONTROL+'.CORE.ENTITY ENTITY    WHERE Err.error_id = Err_LOG.error_id AND Err_LOG.rule_id = Rule.rule_id AND  reprocess_flag = \'Y\' AND RULE.ENTITY_ID = ENTITY.ENTITY_ID  AND CAST(ERR_LOG.CREATION_DATE AS DATE) >= (CASE WHEN ((ENTITY.ALL_EXCP_REPROCESS_DAY_OF_WEEK =			   (SELECT  DAY_OF_WEEK FROM SYS_CALENDAR.CALENDAR WHERE CALENDAR_DATE = DATE)) OR  ENTITY.ALL_EXCP_REPROCESS_DAY_OF_WEEK = 0) THEN CAST(''1600-01-01'' AS DATE)  ELSE DATE - ENTITY.EXCP_AGE_LIMIT_DAILY_REPROCESS END) group by  '+column_list_Err+ ' ,Err.error_id,Err_LOG.error_create_date'  
                     con.cursor().execute(vExecSQL)
			  
			  
       #  sql_text_exec = '''INSERT INTO ' ||   stg_db|| '.' || tt_err_re_tbl  || ' ( ' || column_list||' ,record_id ,error_create_date) ' ||'  SELECT '|| column_list_Err || ' ,Err.error_id,Err_LOG.error_create_date  FROM '|| err_schm_db  +'.'||  error_row_table || ' Err ,'+vPROCESS_CONTROL+'.CORE.'|| error_log_tbl ||' as Err_LOG ,'+vPROCESS_CONTROL+'.CORE.business_rule  RULE , '+vPROCESS_CONTROL+'.CORE.ENTITY ENTITY  WHERE Err.error_id = Err_LOG.error_id AND Err_LOG.rule_id = Rule.rule_id AND  reprocess_flag = ''''Y''''  AND RULE.ENTITY_ID = ENTITY.ENTITY_ID   AND CAST(ERR_LOG.CREATION_DATE AS DATE) >= (CASE WHEN ((ENTITY.ALL_EXCP_REPROCESS_DAY_OF_WEEK =  (SEL  DAY_OF_WEEK FROM SYS_CALENDAR.CALENDAR WHERE CALENDAR_DATE = DATE)) OR  ENTITY.ALL_EXCP_REPROCESS_DAY_OF_WEEK = 0) THEN CAST(''''1600-01-01'''' AS DATE)  ELSE DATE - ENTITY.EXCP_AGE_LIMIT_DAILY_REPROCESS END)   group by  '|| column_list_Err || ' ,Err.error_id,Err_LOG.error_create_date ''' ;##added as part of REQ0103614 ## to capture query executed at this step*/

 
              else:
 
 ## Modified_Dt : 31-Aug-2015 |  Modified_By : Annu | Desc : In Select Query chaned Stg_db to err_schm_db
 ##--Modified_Dt :19-JAN-2016 |Modified_By : NTiwari:group by :In Select Query to avoid duplicate issue--
 
                     vExecSQL='INSERT INTO '+stg_db+'.'+stg_schema+ '.' +tt_err_re_tbl  + ' ( ' +column_list+'  ,record_id,error_create_date) SELECT '+column_list_Err + '  ,Err.error_id ,Err_LOG.error_create_date FROM ( SELECT * FROM '+err_schm_db +'.'+error_row_table + ' WHERE ( '+vParameter_fld+ ') in ( SELECT '+vParameter_fld + '  FROM '   +stg_db+'.'+stg_schema+ '.' +tt_tbl  +' GROUP BY 1 ) ) Err , '+vPROCESS_CONTROL+'.CORE.'+error_log_tbl + ' as Err_LOG , '+vPROCESS_CONTROL+'.CORE.business_rule  RULE , '+vPROCESS_CONTROL+'.CORE.ENTITY ENTITY WHERE Err.error_id = Err_LOG.error_id AND Err_LOG.rule_id = Rule.rule_id AND  reprocess_flag = \'Y\' AND RULE.ENTITY_ID = ENTITY.ENTITY_ID AND CAST(ERR_LOG.CREATION_DATE AS DATE) >= (CASE WHEN ((ENTITY.ALL_EXCP_REPROCESS_DAY_OF_WEEK =			   (SELECT  DAY_OF_WEEK FROM SYS_CALENDAR.CALENDAR WHERE CALENDAR_DATE = DATE)) OR ENTITY.ALL_EXCP_REPROCESS_DAY_OF_WEEK = 0) THEN CAST(''1600-01-01'' AS DATE) ELSE DATE - ENTITY.EXCP_AGE_LIMIT_DAILY_REPROCESS END) group by  '+column_list_Err + ' ,Err.error_id,Err_LOG.error_create_date'
                     con.cursor().execute(vExecSQL)
 
       #  sql_text_exec = '''INSERT INTO ' ||   stg_db|| '.' || tt_err_re_tbl  || ' ( ' || column_list||'  ,record_id,error_create_date) ' ||' SELECT '|| column_list_Err || '  ,Err.error_id ,Err_LOG.error_create_date FROM ( SELECT * FROM '|| err_schm_db +'.'||  error_row_table || ' WHERE ( '|| parameter_fld|| ') in  ( SELECT '|| parameter_fld || '  FROM '||   stg_db|| '.' || tt_tbl  ||'  GROUP BY '|| parameter_fld || ') ) Err , '+vPROCESS_CONTROL+'.CORE.'||error_log_tbl || ' as Err_LOG , '+vPROCESS_CONTROL+'.CORE.business_rule  RULE , '+vPROCESS_CONTROL+'.CORE.ENTITY ENTITY  WHERE Err.error_id = Err_LOG.error_id AND Err_LOG.rule_id = Rule.rule_id AND  reprocess_flag = ''''Y''''  AND RULE.ENTITY_ID = ENTITY.ENTITY_ID  AND CAST(ERR_LOG.CREATION_DATE AS DATE) >= (CASE WHEN ((ENTITY.ALL_EXCP_REPROCESS_DAY_OF_WEEK = (SEL  DAY_OF_WEEK FROM SYS_CALENDAR.CALENDAR WHERE CALENDAR_DATE = DATE)) OR  ENTITY.ALL_EXCP_REPROCESS_DAY_OF_WEEK = 0) THEN CAST(''''1600-01-01'''' AS DATE)  ELSE DATE - ENTITY.EXCP_AGE_LIMIT_DAILY_REPROCESS END)  group by  '|| column_list_Err || ' ,Err.error_id,Err_LOG.error_create_date ''' ;##added as part of REQ0103614 ## to capture query executed at this step*/
 
 
 #END IF;
 
 
              vLog_Message='Temporary error  reprocess table has been populated'
              logProcessAudit(request_id,'10',vLog_Message,vExecSQL)
 

 
 ## LABEL_CREATE_TEMP END;
 
 ## ==============================================================================================
 ##    step 3.2 Insert records into error table
 ## As part of REQ0103614 capture error ids which are delete from error table
 ## ==============================================================================================
 
              vExecSQL='INSERT INTO '   +stg_db+'.'+stg_schema+ '.' +tt_err_log_act_ind_tbl+ '  (error_id,      request_id,       rule_id,        source_record_id,    job_id,               failed_record_attrib01,      failed_record_attrib02 ,      failed_record_attrib03 ,  failed_record_attrib04 ,               failed_record_attrib05, created_by, creation_date,    last_updated_by,      last_updated_date ,               error_create_date ,reprocessed_record, active_ind)               SELECT  error_id,      request_id,       rule_id,        source_record_id,    job_id,               failed_record_attrib01,      failed_record_attrib02 ,      failed_record_attrib03 ,  failed_record_attrib04 ,               failed_record_attrib05, created_by, creation_date,   last_updated_by,      last_updated_date ,               error_create_date ,reprocessed_record, \'N\' as active_ind   FROM '+vPROCESS_CONTROL+'.CORE.'+error_log_tbl+' WHERE ERROR_ID in (SELECT record_id FROM '+stg_db+'.'+stg_schema+ '.' +tt_err_re_tbl+ ' GROUP BY record_id)'
               #added as part of REQ0103614 ## to capture inactive error ids for reprocessable records in temporary error log table*/
              con.cursor().execute(vExecSQL)              
			  
       #  sql_text_exec = '''INSERT INTO ' ||   stg_db|| '.' || tt_err_log_act_ind_tbl  || ' (error_id, request_id, rule_id, source_record_id, job_id, failed_record_attrib01, failed_record_attrib02, failed_record_attrib03 ,  failed_record_attrib04 , failed_record_attrib05, created_by, creation_date,    last_updated_by,      last_updated_date ,  error_create_date ,reprocessed_record, active_ind)  SELECT  error_id,      request_id,       rule_id,        source_record_id,    job_id,  failed_record_attrib01,      failed_record_attrib02 ,      failed_record_attrib03 ,  failed_record_attrib04 ,  failed_record_attrib05, created_by, creation_date,    last_updated_by,      last_updated_date , error_create_date ,reprocessed_record, ''''N'''' as active_ind   FROM '+vPROCESS_CONTROL+'.CORE.'||error_log_tbl||' WHERE ERROR_ID  in ( SELECT record_id FROM ' ||   stg_db|| '.' ||tt_err_re_tbl  || ' GROUP BY record_id )''' ;#added as part of REQ0103614 ## to capture query executed at this step*/
 		

              vLog_Message='Temporary error table has been populated for inactive error ids for reprocessed records'
              logProcessAudit(request_id,'11',vLog_Message,vExecSQL)
			
              vExecSQL='DELETE FROM '+err_schm_db+'.'+error_row_table+'WHERE error_id in (SELECT record_id FROM '+stg_db+'.'+stg_schema+'.'+tt_err_re_tbl+' GROUP BY record_id );'
              con.cursor().execute(vExecSQL)
 #sql_text_exec = '''DELETE FROM '|| err_schm_db +'.' ||error_row_table || ' WHERE error_id in (SELECT record_id FROM ' ||   stg_db|| '.' || tt_err_re_tbl  || ' GROUP BY record_id )  ''';#added as part of REQ0103614 ## to capture query executed at this step*/
 
 
              vLog_Message='Records deleted from error table for which we have been picked up for reprocessing'
              logProcessAudit(request_id,'12',vLog_Message,vExecSQL)
			
 ## ==============================================================================================
 ##  step 1.8 - Only SELECT those records for reprocessing WHERE at least one Rule has Reprocess_Flg = 'Y
 ## ==============================================================================================
 
              vExecSQL='INSERT INTO '   +stg_db+'.'+stg_schema+ '.' +tt_tbl+  ' SELECT ' +column_list+ ', \'Y\' , MAX_id + ROW_NUMBER () over (ORDER BY record_id),error_create_date FROM  ' +stg_db+'.'+stg_schema+ '.' +tt_err_re_tbl+ ' A, ( SELECT COALESCE(MAX(record_id), 0) AS max_id FROM '+stg_db +'.'+stg_schema+ '.' +tt_tbl+' ) B;'
              con.cursor().execute(vExecSQL)
 
 
       #  sql_text_exec ='''INSERT INTO '||   stg_db|| '.' || tt_tbl  ||  ' ' ||'SELECT ' || column_list|| ',' || '''''Y''''' || ', MAX_id + ROW_NUMBER () over (ORDER BY record_id),error_create_date  FROM  '||   stg_db|| '.' || tt_err_re_tbl  || ' A, ( SELECT  ZEROIFNULL (MAX( record_id)) max_id  FROM '|| stg_db +'.' ||  tt_tbl ||' ) B ''';#added as part of REQ0103614 ## to capture query executed at this step*/
  
              vLog_Message='Reprocessable records have been inserted into temporary staging table'
              logProcessAudit(request_id,'13',vLog_Message,vExecSQL)

## ==============================================================================================
## Modified_Dt :08-MAR-2017 |Modified_By : Amit|Desc :All rules repcrecoessing indicator are 'N', skip reprocessing--
## ==============================================================================================

       else: 

              sql_text_exec = 'No query has been executed at this step'  
#added as part of REQ0103614 ## to capture query executed at this step*/

              vLog_Message='Populating temporary error  reprocess table has been skipped as no rules are reprocessable'
              logProcessAudit(request_id,'10',vLog_Message,sql_text_exec)
			
			
              vLog_Message='Deletion from error table for reprocessing has been skipped as no rules are reprocessable'
              logProcessAudit(request_id,'11',vLog_Message,sql_text_exec)
                            
                            
              vLog_Message='Insertion of reprocessable records into temporary staging table have been skipped as as no rules are reprocessable'
              logProcessAudit(request_id,'12',vLog_Message,sql_text_exec)
			
       #END IF;


 
 ## ==============================================================================================
 ##  step 1.12  Create temporary log TABLE 
 ## ==============================================================================================
 
       drop_table(stg_db,stg_schema, tt_err_log_tbl)
 
       vExecSQL='CREATE TABLE '   +stg_db+'.'+stg_schema+ '.' +tt_err_log_tbl+'  LIKE  ( SELECT * FROM '+vPROCESS_CONTROL+'.CORE.'+error_log_tbl+ ' );'
       con.cursor().execute(vExecSQL)
           
           
       sql_text_exec = 'DROP_TABLE ( '+stg_db+', '+tt_err_log_tbl+') | CREATE MULTISET TABLE ' +   stg_db+ '.' + tt_err_log_tbl  +'  LIKE  ( SELECT * FROM '+vPROCESS_CONTROL+'.CORE.'+error_log_tbl+' );'  ;#added as part of REQ0103614 ## to capture query executed at this step*/	   

           
       vLog_Message='Temporary error  log table has been created'
       logProcessAudit(request_id,'14',vLog_Message,sql_text_exec)     
 


 
 ## ==============================================================================================
 ##  step 1.10 DECLARE CURSOR
 ## ==============================================================================================
 
 #LABEL_RULE_LOOP :BEGIN
 
 #DECLARE  c_rule CURSOR  FOR 
 
       c_rule=pd.read_sql_query("SELECT    rule_id,    active_passive_flag,    sql_text,    CASE        WHEN B.DATA_TYPE = 'DATE' THEN 'CAST(TO_CHAR(' || rule_attribute1 || ', \'MM/DD/YYYY\') AS VARCHAR(20))'        ELSE rule_attribute1    END AS rule_attribute1,    CASE        WHEN C.DATA_TYPE = 'DATE' THEN 'CAST(TO_CHAR(' || rule_attribute2 || ', \'MM/DD/YYYY\') AS VARCHAR(20))'        ELSE rule_attribute2    END AS rule_attribute2,    CASE        WHEN D.DATA_TYPE = 'DATE' THEN 'CAST(TO_CHAR(' || rule_attribute3 || ', \'MM/DD/YYYY\') AS VARCHAR(20))'        ELSE rule_attribute3    END AS rule_attribute3,    CASE        WHEN E.DATA_TYPE = 'DATE' THEN 'CAST(TO_CHAR(' || rule_attribute4 || ', \'MM/DD/YYYY\') AS VARCHAR(20))'        ELSE rule_attribute4    END AS rule_attribute4,    CASE        WHEN F.DATA_TYPE = 'DATE' THEN 'CAST(TO_CHAR(' || rule_attribute5 || ', \'MM/DD/YYYY\') AS VARCHAR(20))'        ELSE rule_attribute5    END AS rule_attribute5,    active_passive_flag FROM    (SELECT * FROM "+vPROCESS_CONTROL+".CORE.business_rule     WHERE Enabled_flag = 'Y'       AND DQ_ETL_FLAG = 'D'       AND JOB_ID = "+job_id+"       AND entity_id = "+entity_id+") A LEFT JOIN INFORMATION_SCHEMA.COLUMNS B    ON UPPER(B.TABLE_SCHEMA) = "+stg_db+"    AND UPPER(B.TABLE_NAME) = "+tt_err_tbl+"    AND UPPER(B.COLUMN_NAME) = UPPER(rule_attribute1) LEFT JOIN INFORMATION_SCHEMA.COLUMNS C    ON UPPER(C.TABLE_SCHEMA) = "+stg_db+"    AND UPPER(C.TABLE_NAME) = "+tt_err_tbl+"    AND UPPER(C.COLUMN_NAME) = UPPER(rule_attribute2) LEFT JOIN INFORMATION_SCHEMA.COLUMNS D    ON UPPER(D.TABLE_SCHEMA) = "+stg_db+"    AND UPPER(D.TABLE_NAME) = "+tt_err_tbl+"    AND UPPER(D.COLUMN_NAME) = UPPER(rule_attribute3) LEFT JOIN INFORMATION_SCHEMA.COLUMNS E    ON UPPER(E.TABLE_SCHEMA) = "+stg_db+"    AND UPPER(E.TABLE_NAME) = "+tt_err_tbl+"    AND UPPER(E.COLUMN_NAME) = UPPER(rule_attribute4) LEFT JOIN INFORMATION_SCHEMA.COLUMNS F    ON UPPER(F.TABLE_SCHEMA) = "+stg_db+"    AND UPPER(F.TABLE_NAME) = "+tt_err_tbl+"    AND UPPER(F.COLUMN_NAME) = UPPER(rule_attribute5) ORDER BY rule_id;",engine)
 
       if c_rule.empty:
              vLog_Message="No enabled rules available to execute for Rule Type: "+vRule_Type+" and Entity Id: " + vEntity_ID
              logProcessAudit(vJob_Instance_Id,1,'A',vSource_System,vLog_Message,'Python','SF_Error_Check.sh',0)
              sys.exit()
                
       else:
              sql_text_exec = 'No query has been executed at this step'  
    #added as part of REQ0103614 ## to capture query executed at this step*/
 
              vLog_Message='Start Rule Loop'
              logProcessAudit(request_id,'15',vLog_Message,sql_text_exec)
 
              for index, row in c_rule.iterrows():
                     rule_id = row['rule_id']
                     active_passive_flag = row['active_passive_flag']
                     sql_text = row['sql_text']
                     rule_attribute1 = row['rule_attribute1']
                     rule_attribute2 = row['rule_attribute2']
                     rule_attribute3 = row['rule_attribute3']
                     rule_attribute4 = row['rule_attribute4']
                     rule_attribute5 = row['rule_attribute5']
    # Perform replacements and checks as per original logic
    # This part depends on your implementations of replace_string, Check_Sql_Injection, etc.
    
    # Example of executing a modified SQL text
                     try:
        # Assuming execute_sql is a function you've defined to execute SQL statements
                            execute_sql(sql_text, request_id)
        # Update log with success
                     except Exception as e:
        # Update log with failure
                            continue
  
 # OPEN c_rule;
 
 # FETCH   c_rule 
 # INTO  rule_id, active_passive_flag, sql_text,  rule_attribute1, rule_attribute2, 
    # rule_attribute3,  rule_attribute4,  rule_attribute5,   active_passive_flag;
 
 ## ==============================================================================================
 ##    step 2.1 Execute rules in loop
 ## ==============================================================================================
 

 
        sqlcode = 1
 
 #WHILE ( SQLCODE <> 7632 ) DO
        while sqlcode != 0:
    # Condition 1
            replace_pattern = f"{schm_db}.{entity_name}"
            replace_with = f"{stg_db}.{tt_tbl}"
            sql_text_temp = replace_string(sql_text, replace_pattern, replace_with)
            sql_text = sql_text_temp
            Inject_flag = check_sql_injection(sql_text)

 

        if (  Inject_flag == 1 ): 
 
 ## ==============================================================================================
 ##    step 2.3 Insert/update record into  temp error TABLE 
 ## ============================================================================================== 

        sql_text= 'INSERT INTO ' || stg_db|| '.' || tt_err_tbl  ||'  ('  || column_list  || ',record_id,error_create_date )  '||     
               'SELECT  '|| column_list  || ' , record_id,error_create_date
                FROM ( ' || sql_text_temp || ' ) A ';
   
        sql_text_temp = sql_text_temp.replace("'", "''")
#added as part of REQ0103614 ## to capture query executed at this step*/  

        sql_text_exec= '''INSERT INTO ' || stg_db|| '.' || tt_err_tbl  ||'  ('  || column_list  || ',record_id,error_create_date )  '||  'SELECT  '|| column_list  || ' , record_id,error_create_date  FROM ( '  || sql_text_temp || ' ) A ''';    #added as part of REQ0103614 ## to capture query executed at this step*/'''  
    
        sqlcode = execute_sql_and_check_result(sql_text)
    #con.cursor().execute(sql_text)
    ######################################################################
    # CALL PROCESS_CONTROL.EXECUTE_SQL( 
               # :sql_text
                # ,:request_id
                # ,status_flag
              # );
 

        status_flag, sqlcode = execute_sql_combined(sql_text,request_id)

        if (status_flag == 1 ):
 
        vLog_Message='Rule failed'
        logProcessAudit(request_id,rule_id,vLog_Message,sql_text_exec)  ## v 0003
  
        else              
     
      
    
        vExecSQL='Update ' || +stg_db+'.'+stg_schema+ '.' +tt_err_tbl  ||' 
                SET active_passive_flag = '''+active_passive_flag || ''',rule_id=''' +rule_id  ||''' 
                WHERE rule_id IS NULL  '
                con.cursor().execute(vExecSQL)
    
 ## ==============================================================================================
 ##    step 2.4 Insert records from temporary error table to temp error log table
 ## ==============================================================================================
 
  
        vExecSQL='INSERT INTO ' ||   +stg_db+'.'+stg_schema+ '.' +tt_err_log_tbl  ||' 
                (   error_id,      request_id,       rule_id, source_record_id,
                failed_record_attrib01,      failed_record_attrib02 , failed_record_attrib03 , failed_record_attrib04 ,
                failed_record_attrib05 , job_id,  error_create_date)
               SELECT record_id, '+request_id ||',' +rule_id||',' +source_record_pk || ', ' 
               || COALESCE (:rule_attribute1,'''NA''') ||','
               || COALESCE (:rule_attribute2,'''NA''') ||','
               || COALESCE (:rule_attribute3,'''NA''') ||','
               || COALESCE (:rule_attribute4,'''NA''') ||','
               || COALESCE (:rule_attribute5,'''NA''') ||','
               + job_id  ||' , error_create_date FROM '|| +stg_db+'.'+stg_schema+ '.' +tt_err_tbl  ||
                '  WHERE rule_id = '+rule_id
                con.cursor().execute(vExecSQL)
              
                       
        vLog_Message='Rule Loop'
        logProcessAudit(request_id,rule_id,vLog_Message,sql_text_exec)                
    
 # END IF;
 
        else:
 
        vLog_Message='Sql injection failed'
        logProcessAudit(request_id,rule_id,vLog_Message,sql_text_exec) 
 
 
  
# END IF ;
 
 # FETCH   c_rule
 # INTO   rule_id,     active_passive_flag, sql_text,  rule_attribute1,
     # rule_attribute2,  rule_attribute3,  rule_attribute4,
     # rule_attribute5,  active_passive_flag;
 
 # END WHILE ;
 
        sql_text_exec = '''No query has been executed at this step'''  ;#added as part of REQ0103614 ## to capture query executed at this step*/
 
        vLog_Message='End Rule Loop'
        logProcessAudit(request_id,'16',vLog_Message,sql_text_exec) 
 

 # CLOSE c_rule; 
 
 # END LABEL_RULE_LOOP;
 
 ## ==============================================================================================
 ##Step 3.3 , 3.4  &  3.5  Population of data_quality_actuals - INSERT Records INTO data_quality_actuals 
 ## ==============================================================================================
 
 
 #BEGIN 
 
 ## ==============================================================================================
 ## Forming runtime SELECT statement for all the metrics for the entity
 ## ==============================================================================================
 
        v_stmt_dq_actuals = 'SELECT B.dq_metric_id dq_metric_id, B.rule_id rule_id , zeroifnull (A.fail_count) fail_count 
          FROM ( 
             SELECT rule_id, count(1) fail_count 
             FROM ' || stg_db|| '.' || tt_err_log_tbl || '  A,'  || stg_db|| '.' ||  tt_tbl || ' B 
             WHERE A.error_id = B.record_id 
             AND B.error_record =''N''  group by 1
             ) A
          RIGHT OUTER JOIN
              ( 
              SELECT A.dq_metric_id, A.rule_id
              FROM '+vPROCESS_CONTROL+'.CORE.data_quality_metrics A,' ||  stg_db|| '.' ||  tt_log_tbl  ||' B  
              WHERE A.rule_id = CHECKPOINT_LABEL 
              AND request_id = '|| request_id ||' 
              AND CHECKPOINT_DESC = ''Rule Loop''
              ) B 
           ON A.rule_id = B.rule_id ';
           
           
        v_stmt_dq_actuals = f"""
    SELECT B.dq_metric_id dq_metric_id, B.rule_id rule_id, ZEROIFNULL(A.fail_count) fail_count
    FROM (
      SELECT rule_id, COUNT(1) fail_count
      FROM "{stg_db}"."{tt_err_log_tbl}" A, "{stg_db}"."{tt_tbl}" B
      WHERE A.error_id = B.record_id AND B.error_record = 'N'
      GROUP BY 1
    ) A
    RIGHT OUTER JOIN (
      SELECT A.dq_metric_id, A.rule_id
      FROM "{vPROCESS_CONTROL}".CORE.data_quality_metrics A, "{stg_db}"."{tt_log_tbl}" B
      WHERE A.rule_id = CHECKPOINT_LABEL AND request_id = {request_id} AND CHECKPOINT_DESC = 'Rule Loop'
    ) B
    ON A.rule_id = B.rule_id
    """
 
 
        c_dq_actuals = pd.read_sql_query(v_stmt_dq_actuals, engine)

 ## ==============================================================================================
 ## Dynamic cursor based on above dynamic statement.
 ## ==============================================================================================

        for index, row in c_dq_actuals.iterrows():
            dq_metric_id, rule_id, fail_count = row['dq_metric_id'], row['rule_id'], row['fail_count']
            
            while sqlcode != 0:
            # Perform the calculation as per your logic
            if total_records != 0:
                actual_value = ((total_records - fail_count) / total_records) * 100
            else:
            # If total_records is 0, set the specified values
                total_records = 0
                actual_value = 100
                fail_count = 0
    #END IF;
    
    # Insert into PROCESS_CONTROL.data_quality_actuals
        vExecSQL='INSERT INTO '+vPROCESS_CONTROL+'.data_quality_actuals (dq_metric_id, request_id, dqi_process_flag, actual_value, total_rows, successful_rows, failed_rows, created_by, creation_date, last_updated_by, last_updated_date ) VALUES ('+dq_metric_id+','+request_id+',''N'','+actual_value+','+total_records+','+total_records+'-'+fail_count+','+fail_count+','+modified_by+','+modified_dt+',' +modified_by+','+modified_dt');'"
     
        sqlcode = execute_sql_and_check_result(vExecSQL)
			 
        sql_text_exec = '''SELECT B.dq_metric_id dq_metric_id, B.rule_id rule_id , zeroifnull (A.fail_count) fail_count  FROM (  SELECT rule_id, count(1) fail_count  FROM ' || stg_db|| '.' || tt_err_log_tbl || '  A,'  || stg_db|| '.' ||  tt_tbl || ' B WHERE A.error_id = B.record_id  AND B.error_record =''''N''''  group by 1 ) A    RIGHT OUTER JOIN (  SELECT A.dq_metric_id, A.rule_id   FROM '+vPROCESS_CONTROL+'.CORE.data_quality_metrics A,' ||  stg_db|| '.' ||  tt_log_tbl  ||' B  WHERE A.rule_id = CHECKPOINT_LABEL AND request_id = '|| request_id ||'   AND CHECKPOINT_DESC = ''''Rule Loop''''  ) B     ON A.rule_id = B.rule_id | IF ( total_records <> 0) THEN  SELECT (CAST ( ( :total_records - :fail_count) AS DECIMAL (18,4)) /CAST ( :total_records  AS DECIMAL (18,4))) *100 INTO :actual_value  ; ELSE SET :total_records=0;  SET :actual_value=100;  SET :fail_count=0;  END IF'''  ;#added as part of REQ0103614 ## to capture query executed at this step*/
     
        vLog_Message='Calculate the data qualtiy metric for rule id:'+rule_id+''
        logProcessAudit(request_id,dq_metric_id,vLog_Message,sql_text_exec) 


 
        sql_text_exec = 'No query has been executed at this step'  #;#added as part of REQ0103614 ## to capture query executed at this step*/
 
        vLog_Message='Data Quality Actuals have been calculated'
        logProcessAudit(request_id,'17',vLog_Message,sql_text_exec) 
 

 
 
##LABEL_INSERT_TGT :BEGIN
 
 ## ==============================================================================================
 ##    step 3.1 INSERT records INTO error_log TABLE
 ##    as part of REQ0103614 - inactive error_ids have been updated as 'N'' 
 ## ==============================================================================================
 
        vExecSQL='DELETE FROM '+vPROCESS_CONTROL+'.' +error_log_tbl || '
             WHERE error_id in  (SELECT ERROR_ID FROM ' ||   +stg_db+'.'+stg_schema+ '.' +tt_err_log_act_ind_tbl  ||' GROUP BY ERROR_ID)'
        con.cursor().execute(vExecSQL)
           ##added as part of REQ0103614 ## to delete inactive error ids from ERROR_LOG*/
		   
        sql_text_exec = '''DELETE FROM '+vPROCESS_CONTROL+'.' ||error_log_tbl || ' WHERE error_id in  (SELECT ERROR_ID FROM ' ||   stg_db|| '.' || tt_err_log_act_ind_tbl  ||' GROUP BY ERROR_ID ) '''  ;#added as part of REQ0103614 ## to capture query executed at this step*/
 
        vLog_Message='Inactive error ids from Error log table has been deleted'
        logProcessAudit(request_id,'18',vLog_Message,sql_text_exec)

        vExecSQL='INSERT INTO '+vPROCESS_CONTROL+'.'+error_log_tbl ||'  
             (   error_id,      request_id,       rule_id,        source_record_id,    job_id,
               failed_record_attrib01,      failed_record_attrib02 ,      failed_record_attrib03 ,  failed_record_attrib04 ,
               failed_record_attrib05, created_by, creation_date,    last_updated_by,      last_updated_date ,
               error_create_date ,reprocessed_record,active_ind   )
            SELECT   error_id,      request_id,       rule_id,        source_record_id,    job_id,
               failed_record_attrib01,      failed_record_attrib02 ,      failed_record_attrib03 ,  failed_record_attrib04 ,
               failed_record_attrib05, created_by, creation_date,    last_updated_by,      Current_Timestamp(6) ,
               error_create_date ,reprocessed_record,active_ind
              FROM '||   +stg_db+'.'+stg_schema+ '.' +tt_err_log_act_ind_tbl
              con.cursor().execute(vExecSQL)
              ##added as part of REQ0103614 ## insert inactive error ids with ACTIVE_IND = 'N'*/


        sql_text_exec = '''INSERT INTO '+vPROCESS_CONTROL+'.'||error_log_tbl ||'  (   error_id,      request_id,       rule_id,        source_record_id,    job_id, failed_record_attrib01,      failed_record_attrib02 ,      failed_record_attrib03 ,  failed_record_attrib04 ,  failed_record_attrib05, created_by, creation_date,    last_updated_by,      last_updated_date , error_create_date ,reprocessed_record,active_ind   ) SELECT   error_id,      request_id,       rule_id,        source_record_id,    job_id, failed_record_attrib01,      failed_record_attrib02 ,      failed_record_attrib03 ,  failed_record_attrib04 ,  failed_record_attrib05, created_by, creation_date,    last_updated_by,      Current_Timestamp(6) ,  error_create_date ,reprocessed_record,active_ind     FROM '||   stg_db|| '.' || tt_err_log_act_ind_tbl|| ' '''  ;#added as part of REQ0103614 ## to capture query executed at this step*/
 
        vLog_Message='Inactive error ids in Error log table has been populated with Active_Ind = N'
        logProcessAudit(request_id,'19',vLog_Message,sql_text_exec)
              
        vExecSQL='INSERT INTO '+vPROCESS_CONTROL+'.'+error_log_tbl ||' 
             (   error_id,      request_id,       rule_id,        source_record_id,    job_id,
               failed_record_attrib01,      failed_record_attrib02 ,      failed_record_attrib03 ,  failed_record_attrib04 ,
               failed_record_attrib05, created_by, creation_date,    last_updated_by,      last_updated_date ,
               error_create_date ,reprocessed_record,active_ind    )
            SELECT    request_id ||trim('||  :entity_id_tt ||')||  trim(error_id ) ,       request_id,     rule_id,        source_record_id,  job_id,
              failed_record_attrib01,      failed_record_attrib02 ,      failed_record_attrib03 ,      failed_record_attrib04 ,
              failed_record_attrib05' ||' , '|| ''''  +modified_by || '''' ||',' ||'''' || modified_dt || ''''  ||' , '|| ''''  +modified_by || '''' ||',' ||'''' || modified_dt || '''' ||', 
              CASE WHEN error_create_date IS NULL THEN CAST (  '''+modified_dt ||''' as TIMESTAMP(6))  ELSE error_create_date END,
              CASE WHEN error_create_date IS NULL THEN ''N'' ELSE ''Y'' END, ''Y''
              FROM '||   +stg_db+'.'+stg_schema+ '.' +tt_err_log_tbl
              con.cursor().execute(vExecSQL)
 
 
        sql_text_exec = '''INSERT INTO '+vPROCESS_CONTROL+'.'||error_log_tbl ||'(   error_id, request_id, rule_id,  source_record_id, job_id,failed_record_attrib01, failed_record_attrib02 , failed_record_attrib03 ,  failed_record_attrib04 ,   failed_record_attrib05, created_by, creation_date, last_updated_by, last_updated_date , error_create_date ,reprocessed_record ,active_ind   ) SELECT request_id ||trim('||  entity_id_tt ||')||  trim(error_id ) , request_id,  rule_id,  source_record_id,  job_id,  failed_record_attrib01, failed_record_attrib02 , failed_record_attrib03 , failed_record_attrib04 ,  failed_record_attrib05' ||' , '|| ' '''' '  || modified_by || ' '''' ' ||',' ||' '''' ' || modified_dt || ' '''' '  ||' , '|| ' '''' '  || modified_by || ' '''' ' ||',' || ' '''' ' || modified_dt || ' '''' ' ||',   CASE WHEN error_create_date IS NULL THEN CAST ( '''' '|| modified_dt ||' ''''as TIMESTAMP(6))  ELSE error_create_date END,  CASE WHEN error_create_date IS NULL THEN ''''N'''' ELSE ''''Y'''' END , ''''Y'''' FROM '||   stg_db|| '.' || tt_err_log_tbl||' '''  ;#added as part of REQ0103614 ## to capture query executed at this step*/
 
        vLog_Message='Error log table has been populated from temporary error log table'
        logProcessAudit(request_id,'20',vLog_Message,sql_text_exec)
 
 

 
 ## ==============================================================================================
 ##    step 3.2 Insert records into error table
 ## ==============================================================================================
 
        vExecSQL='DELETE FROM '||  :err_schm_db +'.' +error_row_table || '
             WHERE error_id in (SELECT record_id FROM ' ||   +stg_db+'.'+stg_schema+ '.' +tt_err_re_tbl  || ' GROUP BY record_id)'
             con.cursor().execute(vExecSQL)
 
        vExecSQL='INSERT INTO '||  :err_schm_db +'.' +error_row_table ||' (error_id,' +column_list|| ') 
            SELECT case when B.record_id is null then ' +request_id||' || trim( '||  :entity_id_tt ||') ||  trim(A.record_id ) else B.record_id end,  '+column_list|| '
            FROM '||    +stg_db+'.'+stg_schema+ '.' +tt_tbl  ||'  A
            LEFT OUTER JOIN 
            (SELECT record_id FROM  '||    +stg_db+'.'+stg_schema+ '.' +tt_err_re_tbl  ||' ) B
            ON  B.record_id= A.record_id 
            WHERE A.record_id in (SELECT record_id FROM ' || +stg_db+'.'+stg_schema+ '.' +tt_err_tbl  || ' ) GROUP BY 1 ,'+column_list|| ''
            con.cursor().execute(vExecSQL)
 
 		   
        sql_text_exec = '''DELETE  FROM ' ||   err_schm_db|| '.' || error_row_table  ||' WHERE error_id in (SELECT record_id FROM ' ||   stg_db|| '.' || tt_err_re_tbl  || '  GROUP BY record_id ) | INSERT INTO '||   err_schm_db +'.' || error_row_table ||' (error_id,' || column_list|| ') SELECT  case when B.record_id is null then ' ||  request_id||' || trim( '||   entity_id_tt ||') ||  trim(A.record_id ) else B.record_id end,  '|| column_list|| ' FROM '||     stg_db|| '.' ||  tt_tbl  ||'  A LEFT OUTER JOIN  (SELECT record_id FROM  '||     stg_db|| '.' ||  tt_err_re_tbl  ||' ) B ON  B.record_id= A.record_id  WHERE A.record_id in (SELECT record_id FROM ' ||  stg_db|| '.' ||  tt_err_tbl  || ' ) GROUP BY 1 ,'||column_list|| '  ''' ;#added as part of REQ0103614 ## to capture query executed at this step*/
 
        vLog_Message='Error row table has been populated from temporary error row table '
        logProcessAudit(request_id,'21',vLog_Message,sql_text_exec)

 
 
 ## ==============================================================================================
 ##Step 4.1 Determine the error records with Active Validation  
 ## ==============================================================================================
 
        vExecSQL='DELETE  FROM '||   +stg_db+'.'+stg_schema+ '.' +tt_tbl  || ' 
            WHERE record_id in ( SELECT A.record_id 
                     FROM '||   +stg_db+'.'+stg_schema+ '.' +tt_err_tbl  ||  ' A 
                     WHERE active_passive_flag =''A''  GROUP BY A.record_id)'
                     con.cursor().execute(vExecSQL)
 
 
        sql_text_exec = '''DELETE  FROM ' ||   stg_db|| '.' || tt_tbl  ||' WHERE record_id in (SELECT A.record_id FROM ' ||   stg_db|| '.' || tt_err_tbl  || ' A WHERE active_passive_flag =''''A''''  GROUP BY A.RECORD_ID)  ''' ;#added as part of REQ0103614 ## to capture query executed at this step*/
 
        vLog_Message='Records with active validation have been removed from temporary staging table'
        logProcessAudit(request_id,'22',vLog_Message,sql_text_exec)
 
 

 
 ## ==============================================================================================
 ##step 4.2  DELETE Records FROM Staging for Active Validation
 ## ==============================================================================================
 
        if (staging_Ind == 'Y' ): 
 
 #BEGIN TRANSACTION;
 
        vExecSQL='DELETE FROM '+schm_db+'.'+entity_name+' '+sc_where+' '+select_criteria';'
        con.cursor().execute(vExecSQL)
 
        vExecSQL='INSERT INTO ' +schm_db +'.'+entity_name||' ( '+column_list|| ')  SELECT '+column_list || ' FROM ' ||  +stg_db+'.'+stg_schema+ '.'+tt_tbl+' '+sc_where+' '+select_criteria+';'
        con.cursor().execute(vExecSQL)
     
 #END TRANSACTION;
 
need to check this #select_criteria_temp = select replace(select_criteria,'''','''''');
        #added as part of REQ0103614 ## to capture query executed at this step
        
 
        sql_text_exec = '''DELETE FROM ' ||schm_db +'.'|| entity_name||' '|| sc_where||' '||select_criteria_temp||' | INSERT INTO ' ||schm_db +'.'|| entity_name||' ( '||column_list|| ')  SELECT '|| column_list || ' FROM ' ||  stg_db|| '.' || tt_tbl||' '|| sc_where||' '||select_criteria_temp||' ''' ;#added as part of REQ0103614 ## to capture query executed at this step*/'''
 
        vLog_Message='Records with active validation have been removed from staging table'
        logProcessAudit(request_id,'23',vLog_Message,sql_text_exec)

#END IF ;

 #END LABEL_INSERT_TGT;
 

 ## ==============================================================================================
 #step 4.3 & 4.4  Generate alert if the failure crosses a threshold and send mail
 ## ==============================================================================================
 
 #     Notify_flg= 0;
 
        vExecSQL='DELETE  FROM '+vPROCESS_CONTROL+'.record_count;'
        con.cursor().execute(vExecSQL)
 
        vExecSQL='INSERT INTO '+vPROCESS_CONTROL+'.record_count  SELECT count(1)  total_records FROM ' ||  :stg_db ||'.tt_'||  :entity_name || ' WHERE error_record = ''N'' '
        con.cursor().execute(vExecSQL)
 
        Good_records = pd.read_sql_query("SELECT total_records FROM "+vPROCESS_CONTROL+".record_count;",engine)
        Good_records = Good_records[total_records][0]
 
 
 
## ==============================================================================================
 ##  Exit handler for unhandled exceptions.
 ## ==============================================================================================
except snowflake.connector.ProgrammingError as e:
                # If an exception occurs, set the status flag to 1 and log the error
                #status_flag = 1
                error_code = e.errno
                error_text = e.msg

                if request_id is not None:
                    # Log the error to your check_error_log table, assuming `con.cursor().execute` for insertion
                    cursor.execute("""
                        INSERT INTO PROCESS_CONTROL.check_error_log (request_id, checkpoint_label, checkpoint_desc)
                        VALUES (%s, %s, %s)
                    """, (request_id, error_code, f"{error_text}: {sql_text}"))

except Exception as excp:
        vLog_Message = 'An error has occurred '+ str(excp).replace("'","''")
        sql_text_exec = 'Query execution failed'
        print(vLog_Message)
        logProcessAudit(request_id,rule_id,vLog_Message,sql_text_exec)
        raise


 
except Exception as excp:

## DECLARE EXIT HANDLER FOR SQLEXCEPTION
## BEGIN
 
 
 #sql_text_exec = 'INSERT INTO '+vPROCESS_CONTROL+'.check_error_log ( request_id , checkpoint_label , checkpoint_desc ) VALUES ( ''' + request_id|| ''',''' || error_code  ||''' ,''' || error_text  ||'''  )'''' ; #added as part of REQ0103614 ## to capture query executed at this step*/

vExecSQL='INSERT INTO '+vPROCESS_CONTROL+'.check_error_log( request_id , checkpoint_label , checkpoint_desc,sql_text ) VALUES ( '+request_id+', '+error_code+','+error_text+','+sql_text_exec+' )' 

#Modified as part of REQ0103614 to inject SQL executed in CHECK_ERROR_LOG*/

con.cursor().execute(vExecSQL)
 
drop_table(stg_db,stg_schema,tt_tbl );
 
drop_table(stg_db,stg_schema,:tt_err_tbl  );
 
drop_table(stg_db,stg_schema, :tt_err_re_tbl  );
 
drop_table(stg_db,stg_schema, :tt_err_log_tbl);
 
drop_table(stg_db,stg_schema, :tt_err_log_act_ind_tbl);
 
vExecSQL='INSERT INTO '+vPROCESS_CONTROL+'.check_error_log  select * from  ' ||   +stg_db+|| '.' || +stg_schema+ || '.' || +tt_log_tbl+
con.cursor().execute(vExecSQL)

drop_table(stg_db,stg_schema, :tt_log_tbl);  
 
##END; 
 
finally:
 if ( total_records - Good_records >THRESHOLD_REJECTION ):
 Notify_flg= 1
 #########end of if
 ## ==============================================================================================
 ## Step 5.1 Drop temporary tables
 ## ==============================================================================================
 
 ##LABEL_DROP_TBL :BEGIN
 
drop_table(stg_db,stg_schema,tt_tbl)
drop_table(stg_db,stg_schema,tt_err_tbl)
drop_table(stg_db,stg_schema,tt_err_re_tbl)
drop_table(stg_db,stg_schema,tt_err_log_tbl)
drop_table(stg_db,stg_schema,tt_err_log_act_ind_tbl)
 
 sql_text_exec = '''CALL PROCESS_CONTROL.DROP_TABLE ( '|| stg_db ||', '|| tt_tbl ||' ) |  CALL PROCESS_CONTROL.DROP_TABLE ( '|| stg_db ||' , '|| tt_err_tbl || ' ) | CALL PROCESS_CONTROL.DROP_TABLE ( '|| stg_db || ','|| tt_err_re_tbl || ' ) CALL PROCESS_CONTROL.DROP_TABLE ( ' || stg_db || ', ' || tt_err_log_tbl ||' ) | CALL PROCESS_CONTROL.DROP_TABLE ( '|| stg_db || ', '||tt_log_tbl || ') | CALL PROCESS_CONTROL.DROP_TABLE ( '||stg_db||', '||tt_err_log_act_ind_tbl||') '''  ;#added as part of REQ0103614 ## to capture query executed at this step*/
  
 vLog_Message='Temporary tables have been dropped'
 logProcessAudit(request_id,'24',vLog_Message,sql_text_exec)
 
   vExecSQL='INSERT INTO '+vPROCESS_CONTROL+'.check_error_log  select * from  ' ||   +stg_db+'.'+stg_schema+ '.' +tt_log_tbl
   con.cursor().execute(vExecSQL)
 
drop_table(stg_db,stg_schema, :tt_log_tbl);   
            
 
 #END LABEL_DROP_TBL; 
 
ELSE 
 
 sql_text_exec = '''No query has been executed at this step'''  ; #added as part of REQ0103614 ## to capture query executed at this step*/
 
 vLog_Message='Error processing has been skipped as entity is marked as inactive'
 logProcessAudit(request_id,'25',vLog_Message,sql_text_exec)
  
vExecSQL='INSERT INTO '+vPROCESS_CONTROL+'.check_error_log  select * from  ' ||   +stg_db+'.'+stg_schema+ '.' +tt_log_tbl
con.cursor().execute(vExecSQL)
 
drop_table(stg_db,stg_schema,tt_log_tbl)
            
#END IF;
 
  
#END;
