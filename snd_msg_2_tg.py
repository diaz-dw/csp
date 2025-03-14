## douglas.diaz @ 
## Jan-2023
## Jul-2024
import boto3
from botocore.exceptions import ClientError
import json
import urllib3

def lambda_handler(event, context):
    secret_name = "dev/Tg/XYZBot"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']
    
    
    secDict = json.loads(secret)
    
    
    # Assuming the event contains SQS records
    for record in event['Records']:
        ##print(json.dumps(record))

        # Extract the message body from the SQS record
        sqs_message_body = json.loads(record['body'])

        # Add additional custom data fields to the message
        sqs_message_body['parse_mode'] = 'HTML'
        sqs_message_body['chat_id'] = secDict['BCerBotGroupChatId']
        sqs_message_body['text'] = json.loads(record['body'])

        external_url = 'https://api.telegram.org/bot' + secDict['BCerBotToken'] + '/sendMessage'

        # Send the message in a POST request to the external URL using urllib3
        try:
            http = urllib3.PoolManager()
            headers = {'Content-Type': 'application/json'}
            encoded_data = json.dumps(sqs_message_body).encode('utf-8')
            response = http.request('POST', external_url, body=encoded_data, headers=headers)

            # Print the response for logging purposes
            ##print(f"POST request response: {response.status}, {response.data.decode('utf-8')}")

            # Handle the response as needed
            if response.status == 200:
                print("Message posted successfull to Tg API.")
                
                
                # Delete the message from the SQS queue if the request was successful
                
                # Extract the receipt handle from the SQS record
                receipt_handle = record['receiptHandle']
                # Construct the URL to delete the message from the queue
                q_url = f"https://sqs.{record['awsRegion']}.amazonaws.com/{record['eventSourceARN'].split(':')[4]}/{record['eventSourceARN'].split(':')[5]}"

                sqs_client = boto3.client("sqs")

                # Send the DELETE request to delete the message from the queue
                delete_response = sqs_client.delete_message(QueueUrl=q_url, ReceiptHandle=receipt_handle)

                # Check if the message was successfully deleted from the queue
                if delete_response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    print("Message deleted from the queue successfully.")
                else:
                    print("Failed to delete message from the queue!")
                    print(delete_response['ResponseMetadata']['HTTPStatusCode'])
            else:
                print(f"Unexpected response sending message to Tg API:  {response.status}")

        except Exception as e:
            # Handle exceptions if the request fails
            # f-string (formatted string literal)
            print(f"Error ocurred while processing the message: {str(e)}")

    return {
        'statusCode': 200,
        'body': json.dumps('Lambda function executed successfully!')
    }
