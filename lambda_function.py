import smtplib  
import email.utils
import argparse
import boto3
import urllib3
import os.path
import json
from botocore.exceptions import ClientError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from jinja2 import Environment, BaseLoader
from seronetdBUtilities import *
import datetime
import dateutil.tz





def lambda_handler(event, context):
    s3 = boto3.client("s3")
    ssm = boto3.client("ssm")
    
    # Replace smtp_username with your Amazon SES SMTP user name.
    USERNAME_SMTP = ssm.get_parameter(Name="USERNAME_SMTP", WithDecryption=True).get("Parameter").get("Value")
    
    # Replace smtp_password with your Amazon SES SMTP password.
    PASSWORD_SMTP = ssm.get_parameter(Name="PASSWORD_SMTP", WithDecryption=True).get("Parameter").get("Value")
    
    # If you're using Amazon SES in an AWS Region other than US West (Oregon), 
    # replace email-smtp.us-west-2.amazonaws.com with the Amazon SES SMTP  
    # endpoint in the appropriate region.
    HOST = "email-smtp.us-east-1.amazonaws.com"
    PORT = 587
  
    host = ssm.get_parameter(Name="db_host", WithDecryption=True).get("Parameter").get("Value")
    user = ssm.get_parameter(Name="lambda_db_username", WithDecryption=True).get("Parameter").get("Value")
    dbname = ssm.get_parameter(Name="jobs_db_name", WithDecryption=True).get("Parameter").get("Value")
    password = ssm.get_parameter(Name="lambda_db_password", WithDecryption=True).get("Parameter").get("Value")
    JOB_TABLE_NAME = 'table_file_remover'
    

    
    #get the message from the event
    message = event['Records'][0]['Sns']['Message']
    messageJson = json.loads(message)
    #if the message is passed by the filecopy function
    if(messageJson['previous_function'] == 'filecopy'):
            try:
                #remove the Apostrophe('') in the input
                file_name = messageJson['file_name'].replace("'", '')
                file_added_on = messageJson['file_added_on'].replace("'", '')
                #connect to database to get information needed for slack
                mydb = connectToDB(user, password, host, dbname)
                exe = f"SELECT * FROM {JOB_TABLE_NAME} WHERE file_name = %s AND file_added_on = %s"
                mydbCursor=mydb.cursor(prepared=True)
                mydbCursor.execute(exe, (file_name, file_added_on))
                sqlresult = mydbCursor.fetchone()
                #determine which slack channel to send the message to
                
                if(sqlresult[5] == "COPY_SUCCESSFUL"):
                    file_status="copy successfully"
                elif(sqlresult[5] == "COPY_UNSUCCESSFUL"):
                    file_status="copy unsuccessfully"
                else:
                    print("Error: file has been processed")
                    return #comment this line for testing
                
                
                #file_status="copy successfully"#comment this line in non-prod
                #construct the slack message 
                file_id = str(sqlresult[0])
                file_submitted_by = str(sqlresult[9])
                file_location = str(sqlresult[2])
                file_added_on = str(sqlresult[3])
                file_added_on_list = file_added_on.split(" ")
                file_added_on_date = file_added_on_list[0]
                file_added_on_time = file_added_on_list[1]
                
                # get the HTML template of the email from s3 bucket.
                bucket_email = ssm.get_parameter(Name="bucket_email", WithDecryption=True).get("Parameter").get("Value")
                key_email = ssm.get_parameter(Name="key_email", WithDecryption=True).get("Parameter").get("Value")
                SUBJECT = 'File Received'
                s3_response_object = s3.get_object(Bucket=bucket_email, Key=key_email)
                body = s3_response_object['Body'].read()
                body = body.decode('utf-8')
                template = Environment(loader=BaseLoader).from_string(body)
                BODY_HTML = template.render(file_added_on_date=file_added_on_date, file_added_on_time=file_added_on_time)
                
                #sending the email
                # Replace sender@example.com with your "From" address.
                # This address must be verified with Amazon SES.
                SENDERNAME = 'SeroNet Data Team'
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
                
               
                 
                if(file_submitted_by == "cbc03"):
                    # Replace recipient@example.com with a "To" address. If your account 
                    # is still in the sandbox, this address must be verified.
                    RECIPIENT = ssm.get_parameter(Name="cbc03-recipient-email", WithDecryption=True).get("Parameter").get("Value")  
                    RECIPIENT_LIST = RECIPIENT.split(",")
                   
                    for recipient in RECIPIENT_LIST:
                        msg['To'] = recipient
                        
                        #record the email that sent
                        message_sender_orig_file_id = file_id
                        message_sender_cbc_id = "'"+file_submitted_by+"'"
                        message_sender_recepient = "'"+recipient+"'"
                        eastern = dateutil.tz.gettz('US/Eastern')
                        timestampDB = datetime.datetime.now(tz=eastern).strftime('%Y-%m-%d %H:%M:%S')
                        message_sender_sentdate = "'"+timestampDB+"'"
                        message_sender_sender_email = "'"+SENDER+"'"
                        MESSAGE_SENDER_TABLE_NAME = "table_message_sender"
                        
                        #if copy successfully
                        if file_status == "copy successfully":
                            try:  
                                # Try to send the message.
                                server = smtplib.SMTP(HOST, PORT)
                                server.ehlo()
                                server.starttls()
                                #stmplib docs recommend calling ehlo() before & after starttls()
                                server.ehlo()
                                server.login(USERNAME_SMTP, PASSWORD_SMTP)
                                server.sendmail(SENDER, recipient, msg.as_string())
                                server.close()
                            # Display an error message if something goes wrong.
                            except Exception as e:
                                message_sender_sent_status = "'"+"Email_Sent_Failure"+"'"
                                message_sender_tuple = (message_sender_orig_file_id, message_sender_cbc_id, message_sender_recepient, message_sender_sentdate, message_sender_sender_email, message_sender_sent_status)
                                message_sender_mysql = f"INSERT INTO {MESSAGE_SENDER_TABLE_NAME} VALUES (NULL,%s,%s,%s,%s,%s,%s)"
                                mydbCursor.execute(message_sender_mysql, message_sender_tuple)
                                raise e
                            else:
                                message_sender_sent_status = "'"+"Email_Sent_Success"+"'"
                                message_sender_tuple = (message_sender_orig_file_id, message_sender_cbc_id, message_sender_recepient, message_sender_sentdate, message_sender_sender_email, message_sender_sent_status)
                                message_sender_mysql = f"INSERT INTO {MESSAGE_SENDER_TABLE_NAME} VALUES (NULL,%s,%s,%s,%s,%s,%s)"
                                mydbCursor.execute(message_sender_mysql, message_sender_tuple)
                                print ("Email sent!")
        
                                
                        else:
                            print("Copy is unsuccessful, not sending the email")
                            
            except Exception as e:
                raise e
            finally: 
                #close the connection
                mydb.commit()
                mydb.close()
                
    else:
       print("Right now, the email sender function only support file-remover function.")