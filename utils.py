"""
This module read csv and return list, read and write to S3 and SQS
"""
import csv
import json
import logging

from botocore.exceptions import ClientError

from dataforseo import settings
from dataforseo.settings import BOTO_SESSION, ACCOUNT_ID


class CsvToList:
    """
    Class read csv file from specified location and return list of keywords
    """

    def __init__(self, file_name):
        self.file_name = file_name

    def csv_to_list(self):
        """

        :return: keyword list from csv
        """
        keywords_list = []
        try:
            with open(settings.FILE_PATH.format(self.file_name),
                      "r") as csv_file:
                csv_reader = csv.DictReader(csv_file)
                fields_names = csv_reader.fieldnames
                for keyword_location_pair in csv_reader:
                    item = keyword_location_pair[fields_names[0]].lower()
                    keywords_list.append(item)
            return keywords_list
        except FileNotFoundError as file_not_found:
            logging.error(f"csv file not found {file_not_found}")


class WriteToS3:
    """
    This class write file in s3 bucket
    Input :
        file_name : file path in that bucket example : /home/user/files/latest.json
        bucket_name : s3 bucket example : data.collection
        object_name : default None, the name of the file to the destination example :
        iquanti/keyword metric/date/today.csv
    """

    def __init__(self, file_name, bucket_name, s3_path, object_name=None):
        self.file_name = file_name
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.s3_path = s3_path

    def write_file_to_s3(self):
        """
        This function will write the json file to specified s3 path
        :return: None
        """
        if self.object_name is None:
            self.object_name = self.file_name
        s3_client = BOTO_SESSION.resource('s3')
        try:
            obj = s3_client.Object(self.bucket_name, self.s3_path)
            obj.put(Body=self.file_name)
            logging.info("File uploaded successfully")
        except ClientError as client_error:
            logging.error("file upload failed {}".format(str(client_error)))


class ReadFromS3:
    """
    This class read objects from s3
    """

    def __init__(self, bucket_name, s3_path):
        self.bucket_name = bucket_name
        self.s3_path = s3_path

    def list_all_path_in_s3_folder(self):
        """
        :return: number of object in that folder
        """
        s3_client = BOTO_SESSION.client('s3')
        try:
            response = s3_client.list_objects_v2(Bucket=self.bucket_name,
                                                 Prefix=self.s3_path,
                                                 MaxKeys=100)
            logging.info("successfully read from s3")
            return response.get("Contents")
        except ClientError as client_error:
            logging.error("Failed to read file from s3 {}".format(str(client_error)))


class ReadWriteToSQS:
    """
    This class read and write to AWS SQS queue
    """

    def __init__(self):
        pass

    @staticmethod
    def get_aws_service_connection(service_id):
        """

        :param service_id: SQS or s3
        :return: boto session
        """
        return BOTO_SESSION.client(service_id)

    def __get_queue_url(self, queue_name, account_id=ACCOUNT_ID):
        """
        :param queue_name: SQS queue name
        :param account_id: AWS Account id
        :return: AWS SQS queue url
        """
        client = self.get_aws_service_connection('sqs')
        response = client.get_queue_url(
            QueueName=queue_name,
            QueueOwnerAWSAccountId=account_id).get('QueueUrl')
        return response

    def send_sqs_message(self, param, queue_name):
        """
        :param param: message to be send to sqs
        :param queue_name:SQS queue name
        :return: dict or None
        """
        sqs_client = self.get_aws_service_connection('sqs')
        queue_url = self.__get_queue_url(queue_name)
        try:
            response = sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(param))
            logging.info("Message successfully sent to SQS")
            return response

        except ClientError as client_error:
            logging.error("Failed to send message to sqs queue : {}".format(str(client_error)))

    def retrieve_sqs_messages(self, queue_name):
        """Retrieve messages from an SQS queue
           The retrieved messages are not deleted from the queue.
           :param queue_name: String name of existing SQS queue
           :return: List of retrieved messages. If no messages are available, returned
               list is empty. If error, returns None.
        """
        sqs_client = self.get_aws_service_connection('sqs')
        queue_url = self.__get_queue_url(queue_name)

        try:
            response = sqs_client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1,
                                                  WaitTimeSeconds=20, VisibilityTimeout=600)
            logging.info("Message successfully read from SQS")
            return response

        except ClientError as client_error:
            logging.error("Failed to retrieve message from SQS : {}".format(str(client_error)))

    def delete_sqs_message(self, queue_name, receipt_handle):
        """
        :param queue_name: SQS Queue name
        :param receipt_handle: by which you identify which message to delete
        :return: None
        """
        sqs_client = self.get_aws_service_connection('sqs')
        queue_url = self.__get_queue_url(queue_name)
        try:
            sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            logging.info(
                'message successfully deleted with ReceiptHandle {}'.format(receipt_handle))
        except ClientError as client_error:
            logging.error('Failed to delete message with ReceiptHandle {}'.format(client_error))
