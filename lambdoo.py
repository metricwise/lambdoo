import functools
import http
import json
import logging
import os
import ssl
import xmlrpc.client

import boto3

_logger = logging.getLogger(__name__)

# FIXME DRY magic numbers from odoo/addons/base/controllers/rpc.py
RPC_FAULT_CODE_APPLICATION_ERROR = 1
RPC_FAULT_CODE_WARNING = 2
RPC_FAULT_CODE_ACCESS_DENIED = 3
RPC_FAULT_CODE_ACCESS_ERROR = 4

ssm = None
uid = None


# https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-output-format
def make_response(func):
    @functools.wraps(func)
    def wrapper(event, context):
        global _logger
        _logger.debug("request %s", event)
        try:
            message = func(event, context) or http.HTTPStatus.OK.phrase
            code = http.HTTPStatus.OK.value
        except xmlrpc.client.Fault as e:
            _logger.error(e.faultString)
            message = e.faultString
            code = {
                RPC_FAULT_CODE_WARNING: http.HTTPStatus.BAD_REQUEST.value,
                RPC_FAULT_CODE_ACCESS_DENIED: http.HTTPStatus.UNAUTHORIZED.value,
                RPC_FAULT_CODE_ACCESS_ERROR: http.HTTPStatus.FORBIDDEN.value,
            }.get(e.faultCode, http.HTTPStatus.INTERNAL_SERVER_ERROR.value)
        except xmlrpc.client.ProtocolError as e:
            _logger.error(e.errmsg)
            message = e.errmsg
            code = e.errcode
        except Exception as e:
            _logger.exception(str(e))
            message = http.HTTPStatus.INTERNAL_SERVER_ERROR.phrase
            code = http.HTTPStatus.INTERNAL_SERVER_ERROR.value
        response = {
            'body': json.dumps({'message': message}),
            'statusCode': code,
        }
        _logger.debug("response %s", response)
        return response
    return wrapper


# https://www.odoo.com/documentation/18.0/developer/reference/external_api.html
def execute(model, method, args, kwargs):
    global ssm, uid
    database = os.environ['ODOO_DATABASE']
    context = ssl._create_unverified_context() if os.environ.get('SSL_NO_VERIFY') else None
    host = os.environ['ODOO_HOST']
    password = os.environ['ODOO_PASSWORD']
    user = os.environ['ODOO_USER']

    if password.startswith('/') or password.startswith('arn:'):
        if not ssm:
            ssm = boto3.client('ssm')
        password = ssm.get_parameter(Name=password, WithDecryption=True)['Parameter']['Value']

    if not uid:
        with xmlrpc.client.ServerProxy(f"{host}/xmlrpc/2/common", context=context) as common:
            uid = common.authenticate(database, user, password, {})

    with xmlrpc.client.ServerProxy(f"{host}/xmlrpc/2/object", context=context) as object:
        return object.execute_kw(database, uid, password, model, method, args, kwargs)


# https://docs.aws.amazon.com/lambda/latest/dg/example_serverless_SQS_Lambda_section.html
# https://docs.aws.amazon.com/lambda/latest/dg/example_serverless_SQS_Lambda_batch_item_failures_section.html
def wraps_sqs(func):
    @functools.wraps(func)
    def wrapper(event, context):
        _logger.debug("request %s", event)
        failures = []
        _logger.info("processing %s messages", len(event['Records']))
        for record in event['Records']:
            try:
                _logger.info("processing message %s", record['messageId'])
                result = func(record, context)
                _logger.debug("result %s", result)
            except xmlrpc.client.Fault as e:
                if e.faultCode == RPC_FAULT_CODE_APPLICATION_ERROR:
                    _logger.error(e.faultString)
                    failures.append({'itemIdentifier': record['messageId']})
                else:
                    _logger.warning(e.faultString)
            except xmlrpc.client.ProtocolError as e:
                _logger.error(e.errmsg)
                failures.append({'itemIdentifier': record['messageId']})
            except Exception:
                _logger.exception("unexpected exception")
                failures.append({'itemIdentifier': record['messageId']})
        response = {'batchItemFailures': failures}
        _logger.debug("response %s", response)
        return response
    return wrapper
