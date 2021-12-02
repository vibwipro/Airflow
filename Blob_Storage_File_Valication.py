#*************************************************************************#
#  Script : Blob_Storage_File_Valication.py
#  Developed by : Vibhor
#  Developed Date : 
#  Update Date : 
#  Version No : 
#*************************************************************************#

#************************Import Libaries**********************************#
import airflow, datetime, json, requests
from airflow.utils import timezone
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators import BashOperator, BranchPythonOperator, DummyOperator, PythonOperator, PostgresOperator
from airflow.models import Variable
import xml.etree.ElementTree as ET

#******************Config File Absolute path*****************************************#
env_config_path = Variable.get("LCT_Env_Conf_Path")
header_val = {"Content-Type": "application/json"}

#************************Decleration of Variables**************************#
nextmarker_ack = Variable.get("LCT_NextMarker-ack")
nextmarker_errors = Variable.get("LCT_NextMarker-errors")
File_Verification_API = Variable.get("File_Verification_API") #'http://load-service-monitoring:8080/monitor/fileDetail'

#************************Default Arguments********************************#
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 8, 18),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=3),
}

#*************************DAG Creation**************************************#
dag = DAG('DAG-Blob-Storage-File-Verification',
    schedule_interval='*/10 * * * *',
    default_args=default_args)

#****************** Read JSON file for Config values.*************************************# 
with open(env_config_path + 'Env_Config_File.json', 'r') as E_Conf_json:
    Env_Conf_json = E_Conf_json.read()
    config_env = json.loads(Env_Conf_json)
    
Azure_sastoken = config_env['Azure_sastoken']
Azure_file = config_env['Azure_file']
email_connection_id = config_env['email_connection_id']

#************************Function to fetch Azure sasTokenUri*****************#
def branch_func(**kwargs):
    ti = kwargs['ti']
    xcom_value = ti.xcom_pull(task_ids='get-saastoken')
    print ('*****************************************print*************************************************')
    print (xcom_value)
    sastoken = json.loads(xcom_value)
    d_sastoken = (sastoken['sasTokenUri'])
    #print (d_sastoken)
#************************Writing sasToken to Global Variable******************#
    Variable.set("LCT_SasTokenURI", d_sastoken)
#***********************If token didn't recived then Pipeline will fail*******#
    if d_sastoken is not None:
        return 'get-ack-file'
    else:
        return 'Email'



#****************************Start Dummy operator*****************************#
start_op = DummyOperator(task_id="start-LCT-SaaS-Pipeline", dag=dag)

#****************************Generate sasTokenUri*****************************#
sastoken_op = SimpleHttpOperator(
    task_id='get-saastoken',
    method='GET',
    http_conn_id=Azure_sastoken,
    endpoint='/True',
    data=json.dumps({"sasTokenUri":"string"}),
    xcom_push=True, 
    dag=dag)


#****************collecting sasTokenUri and writing it over Global variable *********#
branch_op = BranchPythonOperator(
    task_id='Calling-Azure-API',
    provide_context=True,
    python_callable=branch_func,
    dag=dag)

#*****************Extracting Azure file Details from 'ack' folder ********************#
if nextmarker_ack:
    endpoint_val = Variable.get("LCT_SasTokenURI").split('//')[1] + '&restype=container&comp=list&prefix=ack'
else:
    endpoint_val = Variable.get("LCT_SasTokenURI").split('//')[1] + '&restype=container&comp=list&prefix=ack&marker=' + nextmarker_ack

    
azurefile_op = SimpleHttpOperator(
    task_id='get-ack-file',
    method='GET',
    http_conn_id=Azure_file,
    endpoint= endpoint_val,
    headers=header_val,
    xcom_push=True, 
    dag=dag)


#*************************Process Azure file by pushing it over file verification API*******************************#
def process_file(**kwargs):
    ti = kwargs['ti']
#************************Collect XML response from Azure API********************************************************#
    print(kwargs['key1'])
    if kwargs['key1'] == 'ack':
        xcom_xml = ti.xcom_pull(task_ids='get-ack-file')
        now = timezone.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        Variable.set("Last_Execution_Time_Run", now)
    elif kwargs['key1'] == 'errors':
        xcom_xml = ti.xcom_pull(task_ids='get-errors-file')
        Variable.set("LCT_SasTokenURI", 'https://Default')
    print ('*****************************************print*************************************************')
#***********************Parse XML Response*************************************************************************#
    myroot = ET.fromstring(xcom_xml)
    nextmarker =myroot.find('NextMarker').text
#***********************Collect & write NextMarker over Global Variable*********************************************#
    Variable.set("LCT_NextMarker-ack", nextmarker)
    print(nextmarker)

    
    date_last_run = Variable.get("Last_Execution_Time")
    last_run_int = int(date_last_run[0:10].replace('-','') + date_last_run[11:20].replace(':',''))
    print (last_run_int)
#***********************Iterate loop to read all files details from XML response************************************#
    for x in myroot.findall('Blobs/Blob'):
        name =x.find('Name').text
        if name.find("/")  > 0:
            file_name =name.split('/')[1]
            file_folde =name.split('/')[0]
            print(file_name, file_folde)
            update_dt =x.find('Properties/Last-Modified').text
            file_update_dt = int(update_dt[12:16] + update_dt[8:11].replace('Jan','01').replace('Feb','02').replace('Mar','03').replace('Apr','04').replace('May','05').replace('Jun','06').replace('Jul','07').replace('Aug','08').replace('Sep','09').replace('Oct','10').replace('Nov','11').replace('Dec','12') + update_dt[5:7] + update_dt[17:25].replace(':',''))
            print(file_update_dt)
            if last_run_int <= file_update_dt:
    #***********************Push extracted file from XML response to File verification API*******************************#
                r = requests.post(File_Verification_API, json={"fileName": file_name, "folderName": file_folde})
                resp = r.status_code
                print(r.status_code)
    #***********************If any file verification response it not equal to 200 then Airflow pipeline fails*************#
                if resp != 200:
                    print ('File' + file_name + 'failed validation over API. Response from API is:' + resp)
                    break
            else:
                print ('Not In Scope')
                resp = 200
        else:
            print ('Missing File Name or Folder Name in : ' + name )
#**************If any file fail over file verification API then entire Airflow pipeline fails**************************#
    if resp == 200 and kwargs['key1'] == 'ack':
        return 'get-errors-file'
    elif resp == 200 and kwargs['key1'] == 'errors':
        cur_now = Variable.get("Last_Execution_Time_Run")
        Variable.set("Last_Execution_Time", cur_now)
        return 'end-LCT-SaaS-Pipeline'
    else:
        return 'Email'
          
file_this = BranchPythonOperator(
    task_id='process_ack_file',
    provide_context=True,
    python_callable=process_file,
    op_kwargs={'key1': 'ack'},
    dag=dag)

#*****************Extracting Azure file Details from 'errors' folder ********************#
if nextmarker_errors:
    endpoint_val = Variable.get("LCT_SasTokenURI").split('//')[1] + '&restype=container&comp=list&prefix=errors'
else:
    endpoint_val = Variable.get("LCT_SasTokenURI").split('//')[1] + '&restype=container&comp=list&prefix=errors&marker=' + nextmarker_errors

    
errors_azurefile_op = SimpleHttpOperator(
    task_id='get-errors-file',
    method='GET',
    http_conn_id=Azure_file,
    endpoint= endpoint_val,
    headers=header_val,
    xcom_push=True, 
    dag=dag)

#****************Process files from folder 'errors'****************************************#
errors_file_this = BranchPythonOperator(
    task_id='process_errors_file',
    provide_context=True,
    python_callable=process_file,
    op_kwargs={'key1': 'errors'},
    dag=dag)


#******************************Email operator *******************************************************
email_op = SimpleHttpOperator(
            task_id='Email',
            method='POST',
            http_conn_id=email_connection_id,
            endpoint='/rest/send/email',
            data=json.dumps({
           "toEmail":"vibhor.gupta2@inter.ikea.com",
           "fromEmail":"no.reply@inter.ikea.com",
           "subject":"ALARM: LCT-Saas Airflow Blob Storage File verification Pipeline has failed",
           "emailBody":" Hi, Airflow -LCT-saas Blob Storage File verification Pipeline has failed. Please take corrective actions.",
           "pathToAttachFile":""
            }),
            headers={"Content-Type": "application/json"},
            trigger_rule='one_failed',
            dag=dag)


end_op = DummyOperator(task_id="end-LCT-SaaS-Pipeline", dag=dag)

#**************************Design Flow of DAG's***************************************#
start_op >>  sastoken_op >> branch_op >> azurefile_op >> file_this >> errors_azurefile_op >> errors_file_this >> end_op
sastoken_op >> email_op
branch_op >> email_op
azurefile_op  >> email_op
file_this >> email_op
errors_azurefile_op >> email_op
errors_file_this >> email_op