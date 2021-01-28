import smtplib  
import email.utils
import argparse
import boto3
import urllib3
import os.path
from botocore.exceptions import ClientError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from jinja2 import Environment, BaseLoader
from seronetdBUtilities import *
import datetime
import dateutil.tz



s3 = boto3.client("s3")
ssm = boto3.client("ssm")


#BUCKET_NAME="data-validation-output-bucket"
#KEY="CBC_Name/2021-01-22-11-28_New_Testing_Data_Dirty.zip/New_Testing_Data_Dirty.zip"
#FILE_NAME = os.path.basename(KEY) 
#TMP_FILE_NAME = '/tmp/' +FILE_NAME
#s3.download_file(BUCKET_NAME, KEY, TMP_FILE_NAME)
#ATTACHMENT = TMP_FILE_NAME


# Replace smtp_username with your Amazon SES SMTP user name.
USERNAME_SMTP = ssm.get_parameter(Name="USERNAME_SMTP", WithDecryption=True).get("Parameter").get("Value")

# Replace smtp_password with your Amazon SES SMTP password.
PASSWORD_SMTP = ssm.get_parameter(Name="PASSWORD_SMTP", WithDecryption=True).get("Parameter").get("Value")

# If you're using Amazon SES in an AWS Region other than US West (Oregon), 
# replace email-smtp.us-west-2.amazonaws.com with the Amazon SES SMTP  
# endpoint in the appropriate region.
HOST = "email-smtp.us-east-1.amazonaws.com"
PORT = 587

def lambda_handler(event, context):
  
    http=urllib3.PoolManager()
    host=ssm.get_parameter(Name="db_host", WithDecryption=True).get("Parameter").get("Value")
    user=ssm.get_parameter(Name="lambda_db_username", WithDecryption=True).get("Parameter").get("Value")
    dbname = ssm.get_parameter(Name="jobs_db_name", WithDecryption=True).get("Parameter").get("Value")
    password=ssm.get_parameter(Name="lambda_db_password", WithDecryption=True).get("Parameter").get("Value")
    job_table_name='table_file_remover'
    

    
    #get the message from the event
    message = event['Records'][0]['Sns']['Message']
    #print(event)
    messageJson=eval(message)

    #if the message is passed by the filecopy function
    if(messageJson['previous_function']=='filecopy'):
        

        #connect to database to get information needed for slack
        mydb=connectToDB(user, password, host, dbname)
        exe="SELECT * FROM "+job_table_name+" WHERE file_name="+messageJson['file_name']+" AND "+"file_added_on="+messageJson['file_added_on']
        mydbCursor=mydb.cursor()
        mydbCursor.execute(exe)
        sqlresult = mydbCursor.fetchone()
        #determine which slack channel to send the message to
        if(sqlresult[5]=="COPY_SUCCESSFUL"):
            file_status="copy successfully"
        elif(sqlresult[5]=="COPY_UNSUCCESSFUL"):
            file_status="copy unsuccessfully"
        
        #construct the slack message 
        file_name=str(sqlresult[1])
        file_id=str(sqlresult[0])
        file_submitted_by=str(sqlresult[9])
        file_location=str(sqlresult[2])
        file_added_on=str(sqlresult[3])
        
        # get the HTML template of the email from s3 bucket.
        bucket_email="bruce-email-templates"
        key_email="file-remover-success-template.html"
        SUBJECT = 'Email notifications from file-remover'
        s3_response_object = s3.get_object(Bucket=bucket_email, Key=key_email)
        body = s3_response_object['Body'].read()
        body=body.decode('utf-8')
        template = Environment(loader=BaseLoader).from_string(body)
        #template = env.get_template(key_email)
        BODY_HTML =template.render(file_name=file_name, file_id=file_id, file_submitted_by=file_submitted_by, file_location=file_location, file_added_on=file_added_on,file_status=file_status)
        
        #sending the email
        # Replace sender@example.com with your "From" address.
        # This address must be verified with Amazon SES.
        SENDERNAME = 'Fuyuan Wang'
        SENDER = ssm.get_parameter(Name="sender-email", WithDecryption=True).get("Parameter").get("Value")
        # Create message container - the correct MIME type is multipart/alternative.
        msg = MIMEMultipart('alternative')
        msg['Subject'] = SUBJECT
        msg['From'] = email.utils.formataddr((SENDERNAME, SENDER))
        
        # Record the MIME types of both parts - text/plain and text/html.
        part1 = MIMEText(BODY_HTML, 'html')
        # Attach parts into message container.
        # According to RFC 2046, the last part of a multipart message, in this case
        # the HTML message, is best and preferred.
        msg.attach(part1)
        
        # Define the attachment part and encode it using MIMEApplication.
        #att = MIMEApplication(open(ATTACHMENT, 'rb').read())
        # Add a header to tell the email client to treat this part as an attachment,
        # and to give the attachment a name.
        #att.add_header('Content-Disposition','attachment',filename=os.path.basename(ATTACHMENT))
        # Add the attachment to the parent container.
        #msg.attach(att)
         
        if(file_submitted_by=="cbc03"):
            # Replace recipient@example.com with a "To" address. If your account 
            # is still in the sandbox, this address must be verified.
            RECIPIENT = ssm.get_parameter(Name="cbc03-recipient-email", WithDecryption=True).get("Parameter").get("Value")  
            RECIPIENT_LIST= RECIPIENT.split(",")
           
            for i in range(0,len(RECIPIENT_LIST)):
                msg['To'] = RECIPIENT_LIST[i]
                
                #record the email that sent
                message_sender_orig_file_id=file_id
                message_sender_cbc_id="'"+file_submitted_by+"'"
                message_sender_recepient="'"+RECIPIENT_LIST[i]+"'"
                eastern = dateutil.tz.gettz('US/Eastern')
                timestamp=datetime.datetime.now(tz=eastern).strftime("%H-%M-%S-%m-%d-%Y")
                timestampDB=datetime.datetime.now(tz=eastern).strftime('%Y-%m-%d %H:%M:%S')
                message_sender_sentdate="'"+timestampDB+"'"
                message_sender_sender_email="'"+SENDER+"'"
                message_sender_table_name="table_message_sender"
                
                
                
                try:  
                    # Try to send the message.
                    server = smtplib.SMTP(HOST, PORT)
                    server.ehlo()
                    server.starttls()
                    #stmplib docs recommend calling ehlo() before & after starttls()
                    server.ehlo()
                    server.login(USERNAME_SMTP, PASSWORD_SMTP)
                    server.sendmail(SENDER, RECIPIENT_LIST[i], msg.as_string())
                    server.close()
                # Display an error message if something goes wrong.
                except Exception as e:
                    message_sender_sent_status="'"+"Email_Sent_Failure"+"'"
                    message_sender_tuple=(message_sender_orig_file_id, message_sender_cbc_id, message_sender_recepient, message_sender_sentdate, message_sender_sender_email, message_sender_sent_status)
                    message_sender_mysql="INSERT INTO "+ message_sender_table_name+" VALUES (NULL,%s,%s,%s,%s,%s,%s)" %message_sender_tuple
                    executeDB(mydb,message_sender_mysql)
                    raise e
                else:
                    message_sender_sent_status="'"+"Email_Sent_Success"+"'"
                    message_sender_tuple=(message_sender_orig_file_id, message_sender_cbc_id, message_sender_recepient, message_sender_sentdate, message_sender_sender_email, message_sender_sent_status)
                    message_sender_mysql="INSERT INTO "+ message_sender_table_name+" VALUES (NULL,%s,%s,%s,%s,%s,%s)" %message_sender_tuple
                    executeDB(mydb,message_sender_mysql)
                    print ("Email sent!")

