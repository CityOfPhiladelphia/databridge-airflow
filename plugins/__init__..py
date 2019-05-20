from airflow.plugins_manager import AirflowPlugin

import operators


class CartoPlugin(AirflowPlugin):
    name = 'carto_plugin'
    operators = [operators.S3ToCartoOperator]

class DatabridgePlugin(AirflowPlugin):
    name = 'databridge_plugin'
    operators = [operators.DataBridgeToS3Operator, operators.S3ToDataBridge2Operator]

class KnackPlugin(AirflowPlugin):
    name = 'knack_plugin'
    operators = [operators.KnackToS3Operator]

class SlackPlugin(AirflowPlugin):
    name = 'slack_notify_plugin'
    operators = [operators.SlackNotificationOperator]
