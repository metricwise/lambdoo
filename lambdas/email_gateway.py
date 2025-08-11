# Inspired by https://github.com/odoo/odoo/blob/15.0/addons/mail/static/scripts/odoo-mailgate.py

import json
import logging

import boto3

from lambdoo import execute, wraps_sqs

_logger = logging.getLogger(__name__)

s3 = boto3.client('s3')


@wraps_sqs
def sqs_email_gateway(event, context):
    body = json.loads(event['body'])
    bucket = body['receipt']['action']['bucketName']
    key = body['receipt']['action']['objectKey']
    _logger.info("processing email s3://%s/%s", bucket, key)
    message = s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8')
    execute('mail.thread', 'message_process', [False, message], {})
