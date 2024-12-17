# Inspired by https://github.com/odoo/odoo/blob/15.0/addons/mail/static/scripts/odoo-mailgate.py

import logging
import os
import xmlrpc.client

import boto3

from lambdoo import execute, make_response

_logger = logging.getLogger(__name__)

s3 = boto3.client('s3')


@make_response
def email_gateway(event, context):
    bucket = os.environ['BUCKET']
    key_prefix = os.environ['KEY_PREFIX']
    for record in event['Records']:
        key = record['ses']['mail']['messageId']
        if key_prefix:
            key = f"{key_prefix}/{key}"
        _logger.info("processing email s3://%s/%s", bucket, key)
        msg = s3.get_object(Bucket=bucket, Key=key)['Body'].read()
        execute('mail.thread', 'message_process', [False, xmlrpc.client.Binary(msg)], {})
