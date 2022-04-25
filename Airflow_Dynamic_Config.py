#*************************************************************************#
#  Script : Airflow_Dynamic_Config.py
#  Developed by : Vibhor
#  Developed Date : 2021-04-10
#  Update Date : 2021-04-10
#  Version No : 1.00.01
#*************************************************************************#
# Version No : 1.00.01 : Draft version.
#*************************************************************************#

#************************Import Libaries**********************************#
import json, airflow, datetime, calendar
from datetime import date, timedelta
from time import gmtime, strftime
from airflow import DAG
from airflow.sensors.sql_sensor import SqlSensor
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import BashOperator, BranchPythonOperator
from airflow.operators.sensors import HttpSensor, ExternalTaskSensor
from airflow.models import Variable

#******************Config File Absolute path*****************************************#
env_config_path = Variable.get("LCB_Env_Conf_Path")
config_path = Variable.get("LCB_Conf_Path")

#******************Default Values****************************************************#
one_hour = 60 * 60
select_sql = "SELECT 1 FROM lcb_transformation_history WHERE "
join_sub_category = "' AND subcategory='"
order_rec = "' AND created_timestamp BETWEEN current_date AND NOW() ORDER BY created_timestamp DESC LIMIT 1"
order_rec_delta = "' AND NOT transfer_completed AND created_timestamp BETWEEN current_date AND NOW() ORDER BY created_timestamp DESC LIMIT 1), 0) as Val FROM DUAL ) A"
parent_task_sql ="' and fullload_transformation_status = 'OK' and deltaload_transformation_status = 'OK'  and created_timestamp BETWEEN current_date "
parent_task_order ="and NOW() ORDER BY created_timestamp DESC LIMIT 1"

#****************** Create Dynamic DAG Request.***************************************#

def create_dag(dag_id,
               schedule,
               dag_number,
               default_args):


    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args)
    
#******************** Capturing Category name from DAG_id*********************************#
    category = dag_id.split("_")[3]
    

#****************** Read JSON file for Config values.*************************************# 
    with open(env_config_path + 'Env_Config_File.json', 'r') as E_Conf_json:
        env_conf_json = E_Conf_json.read()
        config_env = json.loads(env_conf_json)
       
    with open(config_path + 'Config_Template.json', 'r') as T_Conf_json:
        file_content = T_Conf_json.read()
        spark_config_template = json.loads(file_content)
        
    with open(config_path + 'Config_File.json', 'r') as Conf_json:
        conf_json1 = Conf_json.read()
        config_template = json.loads(conf_json1)


        
#************Default Values **************************************************************#
    header_value = {"Content-Type": "application/json"}
        
#************Extract Environment variables from Environment Config File*******************#
    sparkjob_api = config_env['Sparkjob-API']
    statedb = config_env['StateDB']
    controm_m_status = config_env['controm_M_status']
    email_connection_id = config_env['email_connection_id']
    to_email = config_env['to_email']
    from_email = config_env['from_email']
    
    
#*********** Assigning enviroment variables for each category to Config template**********#         
    conf_data = config_template[category]
    spark_config_template['spec']['driver']['env'] = conf_data['env']
    spark_config_template['metadata']['namespace'] = config_env['namespace']
    
#***********Assign cores from Config File*************************************************#    
    if conf_data.get('cores', 'null') != 'null':
        spark_config_template['spec']['executor']['cores'] = conf_data['cores']   
        
#***********Assign Memory from Config File*************************************************#    
    if conf_data.get('memory', 'null') != 'null':
        spark_config_template['spec']['executor']['memory'] = conf_data['memory'] 

#***********Assign memoryOverhead from Config File*************************************************#    
    if conf_data.get('memoryOverhead', 'null') != 'null':
        spark_config_template['spec']['executor']['memoryOverhead'] = conf_data['memoryOverhead']
    
#*********** Extract Sub-category if exist, else Null*************************************# 
    category_job = category
    category = category.split("-")[0]
    subcategory = conf_data.get('subcategory', 'null')
        
#****************** DAG Creation starts.*************************************************#
    with dag:
        start_op = DummyOperator(task_id="start-LCB-SaaS-Pipeline")
#**************************Email DAG************************************************#
        email_op = SimpleHttpOperator(
            task_id='Email',
            method='POST',
            http_conn_id=email_connection_id,
            endpoint='/rest/send/email',
            data=json.dumps({
           "toEmail":to_email,
           "fromEmail":from_email,
           "subject":"ALARM: LCB-Saas Airflow Pipeline has failed for category: " + category + " and subcategory: " + subcategory,
           "emailBody":""" Hi,\n\n Airflow -LCB-Saas Pipeline has failed. Please take corrective actions. 
           Detauls of the pipeline line are :-\n
           DAG Name:""" + dag_id + ", category: " + category + ", and subcategory: " + subcategory,
           "pathToAttachFile":""
            }),
            headers=header_value,
            trigger_rule='one_failed',
            dag=dag)
            
#*******************Branch Operator for Pre-processing of data****************************#        
        def branch_func(**kwargs):
            pp_status = conf_data['Pre-Processing']
            if pp_status == 'True':
                return 'delete-existing-pre-processing-app'
            else:
                return 'Pass'        
        
        
        branch_op = BranchPythonOperator(
            task_id='whether-to-bypass-preprocessing',
            provide_context=True,
            python_callable=branch_func,
            trigger_rule='all_success',
            dag=dag)
        
#**********************Trigger Weekend run***********************************#
        if (conf_data.get('execution-day', 'null') != 'null') :
    
            my_date = date.today()
            exe_day = calendar.day_name[my_date.weekday()]

            def proceed(**context):
                if exe_day == 'Saturday' and conf_data.get('execution-day', 'null') == 'Saturday' :
                    return True
                elif exe_day == 'Monday' and conf_data.get('execution-day', 'null') == 'Monday' :
                    return True
                elif exe_day != 'Monday' and conf_data.get('execution-day', 'null') == '!Monday' :
                    return True
                elif exe_day != 'Saturday' and conf_data.get('execution-day', 'null') == '!Saturday' :
                    return True
                elif exe_day == 'Tuesday' and conf_data.get('execution-day', 'null') == 'Tuesday' :
                    return True
                elif exe_day == 'Wednesday' and conf_data.get('execution-day', 'null') == 'Wednesday' :
                    return True  
                elif exe_day == 'Sunday' and conf_data.get('execution-day', 'null') == 'Sunday' :
                    return True
                elif exe_day != 'Sunday' and conf_data.get('execution-day', 'null') == '!Sunday' :
                    return True    
                else:
                    return False
            
            Weekend_gate = ShortCircuitOperator(
            task_id='Weekend-execution-preprocessing',
            python_callable=proceed
            )
            Weekend_gate >> start_op

#**********************Parent-Child-dependency***********************************#
        if conf_data.get('waiting-DAG', 'null') != 'null' :
            if 'inventoryMeasure-s' in category_job:
                sql_upstream = select_sql + "category = '" + category + "' and subcategory LIKE '" + conf_data.get('waiting-DAG', 'null') + parent_task_sql + "- 1 " + parent_task_order
            elif 'inventoryMeasure-NA' in category_job: 
                sql_upstream = "select case when (select count(1) from lcb_transformation_history lth  where category = '" + category + "' and subcategory like '" + conf_data.get('waiting-DAG', 'null') + "' and fullload_transformation_status='OK' and deltaload_transformation_status='OK' AND created_timestamp BETWEEN current_date AND NOW()) = 5 then 1 else 0 end from dual"
            else:
                sql_upstream = select_sql + "category = '" + category + "' and subcategory LIKE '" + conf_data.get('waiting-DAG', 'null') + parent_task_sql + parent_task_order


            upstream_check = SqlSensor(task_id="check-whether-upstream-dag-is-complete", 
                                    poke_interval=120, 
                                    conn_id=statedb, 
                                    sql=sql_upstream, 
                                    timeout=4*one_hour,
                                    retries=0,
                                    dag=dag)
            upstream_check >> start_op 
          
#******************Control-M Job status.*************************************************#    
    #*************fetch Job if from Config file******************************#
        if conf_data.get('pre-condition-jobid', 'null') != 'null':
          job_list1 = conf_data['pre-condition-jobid'] 
          job_list = json.loads(job_list1.replace("\'", "\""))    

          #*************Calculate Order Date******************************#
          if conf_data.get('control-m-date', 'null') == 'TODAY' and int(strftime("%H%M%S", gmtime())) < 50000:
              order_date = (date.today() - timedelta(1)).strftime("%y%m%d")
          elif conf_data.get('control-m-date', 'null') == 'TODAY':
              order_date = date.today().strftime("%y%m%d")
          else:
              order_date = (date.today() - timedelta(1)).strftime("%y%m%d")

          for job in job_list:
              sensor = HttpSensor(
                  task_id='Control_M_status_{0}'.format(job.replace('#','')),
                  http_conn_id=controm_m_status,
                  endpoint=job + '/orderDate/' + order_date,
                  request_params={},
                  response_check=lambda response: True if b"True" in response.content else False,
                  headers={},
                  timeout=6*one_hour,
                  xcom_push=False,
                  poke_interval=300,
                  mode="reschedule",
                  retries=0,
                  dag=dag)
              start_op >> sensor >> branch_op
              sensor >> email_op
        else:
            start_op >> branch_op

#******************Pre-processing Part starts Here **************************************# 
#****************** Delate existing Pre-processing POD.*********************************#  
        delete_pp_op = SimpleHttpOperator(
            task_id='delete-existing-pre-processing-app',
            method='DELETE',
            http_conn_id=sparkjob_api,
            endpoint='/lcb-pp-' + category_job.lower(),
            headers=header_value,
            soft_fail=True,
            retries=0,
            dag=dag)

      
        spark_config_template['metadata']['name'] = 'lcb-pp-' + category_job.lower() 
        if conf_data.get('Pre-processing-class','null') != 'null':
            spark_config_template['spec']['mainClass'] = conf_data['Pre-processing-class']
        
#****************** Pre-processing triggering POD.*********************************#  
        if conf_data['Pre-Processing'] == 'True':
            trigger_rule_v='all_done'
        else:
            trigger_rule_v='one_success'

        create_pp_op = SimpleHttpOperator(
            task_id='trigger-pre-processing-app',
            method='POST',
            http_conn_id=sparkjob_api,
            endpoint='',
            data=json.dumps(spark_config_template),
            headers=header_value,
            retries=0,
            trigger_rule=trigger_rule_v,
            dag=dag)     
    
    
#****************** SQL sensor for verification of Pre-Processing Job completion.******#    
        sql = select_sql + "deltaload_transformation_status is null AND fullload_transformation_status is null AND preprocessing_status = 'OK' AND category='" + category + join_sub_category + subcategory + order_rec
    

        pre_proc_check = SqlSensor(task_id="check-whether-pre-processing-is-complete", 
                                poke_interval=300, 
                                mode="reschedule", 
                                conn_id=statedb, 
                                sql=sql, 
                                timeout=3*one_hour,
                                trigger_rule='one_success',
                                retries=0,
                                dag=dag)

#******************Tranformation Part starts Here **************************************#    
#****************** Delate existing transformation POD.*********************************#        
        delete_op = SimpleHttpOperator(
            task_id='delete-existing-transformation-app',
            method='DELETE',
            http_conn_id=sparkjob_api,
            endpoint='/lcb-spark-' + category_job.lower(),
            headers=header_value,
            soft_fail=True,
            trigger_rule='one_success',
            retries=0,
            dag=dag)
        
#****************** Trigger Transformation job.*****************************************# 

        #****************Update SPark job name & yaml file name ************************#
        spark_config_template['metadata']['name'] = 'lcb-spark-' + category_job.lower()
        spark_config_template['spec']['mainClass'] = 'com.company.lcb.TransformApplication'

        
        #****************Trigger code***************************************************#
        create_op = SimpleHttpOperator(
            task_id='trigger-transformation-app',
            method='POST',
            http_conn_id=sparkjob_api,
            endpoint='',
            data=json.dumps(spark_config_template),
            headers=header_value,
            retries=0,
            trigger_rule='all_done',
            dag=dag)

#****************** SQL sensor for verification of Transformation Job completion.******#     
        if conf_data['Pre-Processing'] == 'True':
            trans_sql = select_sql + "deltaload_transformation_status is null AND fullload_transformation_status = 'OK' AND preprocessing_status = 'OK' AND category='" + category + join_sub_category + subcategory + order_rec
        else:
            trans_sql = select_sql + "deltaload_transformation_status is null AND fullload_transformation_status = 'OK' AND category='" + category + join_sub_category + subcategory + order_rec
    

        transform_check = SqlSensor(task_id="check-whether-transformation-is-complete", 
                                poke_interval=300, 
                                mode="reschedule", 
                                conn_id=statedb, 
                                sql=trans_sql, 
                                timeout=5*one_hour,
                                retries=0,
                                dag=dag)
        
#****************** Replace Config values of JSON file for Delta job.****************#
        
        spark_config_template['metadata']['name'] = 'lcb-delta-' + category_job.lower()
        spark_config_template['spec']['mainClass'] = 'com.company.lcb.DeltaApplication'


#****************** Delete existing Delta POD.***************************************#
        delete_delta = SimpleHttpOperator(
            task_id='delete-existing-delta-app',
            method='DELETE',
            http_conn_id=sparkjob_api,
            endpoint='/lcb-delta-' + category_job.lower(),
            headers=header_value,
            soft_fail=True,
            retries=0,
            dag=dag)

#****************** Trigger Delta job.*******************************************************#
        delta_op = SimpleHttpOperator(
            task_id='trigger-delta',
            method='POST',
            http_conn_id=sparkjob_api,
            endpoint='',
            data=json.dumps(spark_config_template),
            headers=header_value,
            trigger_rule='all_done',
            retries=0,
            dag=dag)
        
#****************** SQl sensor for Delta job completion.***************************************#
        if conf_data['Pre-Processing'] == 'True':
            delta_sql = "SELECT CASE WHEN A.Val < 0 THEN 0 ELSE A.Val END FROM ( SELECT coalesce ((" + select_sql + "deltaload_transformation_status = 'OK' AND fullload_transformation_status = 'OK' AND preprocessing_status = 'OK' AND category='" + category + join_sub_category + subcategory + "' AND NOT transfer_completed AND created_timestamp BETWEEN current_date AND NOW() ORDER BY created_timestamp DESC LIMIT 1), 0) - coalesce((" + select_sql + "deltaload_transformation_status IS NULL AND fullload_transformation_status = 'OK' AND preprocessing_status = 'OK' AND category='" + category + join_sub_category + subcategory + order_rec_delta
        else:
            delta_sql = "SELECT CASE WHEN A.Val < 0 THEN 0 ELSE A.Val END FROM ( SELECT coalesce ((" + select_sql + "deltaload_transformation_status = 'OK' AND fullload_transformation_status = 'OK' AND category='" + category + join_sub_category + subcategory + "' AND NOT transfer_completed AND created_timestamp BETWEEN current_date AND NOW() ORDER BY created_timestamp DESC LIMIT 1), 0) - coalesce((" + select_sql + "deltaload_transformation_status IS NULL AND fullload_transformation_status = 'OK' AND category='" + category + join_sub_category + subcategory + order_rec_delta
        

        delta_check = SqlSensor(task_id="check-whether-delta-is-complete", 
                            poke_interval=300, 
                            mode="reschedule", 
                            conn_id=statedb, 
                            sql=delta_sql, 
                            timeout=2*one_hour,
                            retries=0,
                            dag=dag)
    
#****************** Dummy Operator.*************************************************#
    
        end_op = DummyOperator(task_id="I-am-done")
        pass_op = DummyOperator(task_id="Pass")
              
        
#****************** Task flow design.***********************************************************#     
    branch_op >> delete_pp_op
    branch_op >> pass_op
    [ (delete_pp_op >> create_pp_op >> pre_proc_check), pass_op] >> delete_op >> create_op >> transform_check >> delete_delta >> delta_op >> delta_check >> end_op
#*******************Exception Handeling.*********************************************************#
    create_pp_op >> email_op
    pre_proc_check >> email_op
    create_op >> email_op
    transform_check >> email_op
    delta_op >> email_op
    delta_check >> email_op
    
    
    return dag

#****************** Create list of categoeries.*******************************************************#
list = ['location', 'inventoryMeasure-s5', 'inventoryMeasure-APAC','inventoryMeasure-Cff-NA','inventoryMeasure-Cff-APAC','inventoryMeasure-Cff-Supplier', 'itemLocation-AP','transportInstruction-supplyDCG','inventoryMeasure-NA-nfco', 'inventoryMeasure-NA-blockedstock']

#****************** build a dag for each categoery listed above.***************************************#
for idx, list in enumerate(list):
    dag_id = 'DAG_LCB_SaaS_{}'.format(str(list))

    default_args = {'owner': 'airflow',
                    'start_date': airflow.utils.dates.days_ago(1),
                    'depends_on_past': False,
                    'retries': 1,
                    'retry_delay': datetime.timedelta(hours=5)
                    }

#****************** Extracting Scheduling from Config file.***************************************#    
    with open(config_path + 'Config_File.json', 'r') as Conf_schedul:
        Conf_schedul1 = Conf_schedul.read()
        Conf_schedul_template = json.loads(Conf_schedul1)    

    schedule = Conf_schedul_template[list]['schedule']

    dag_number = idx

    globals()[dag_id] = create_dag(dag_id,
                                  schedule,
                                  dag_number,
                                  default_args)
