from airflow.plugins_manager import AirflowPlugin

from plugins.operators import (
    S3ToCartoOperator,
    DataBridgeToS3Operator, S3ToDataBridge2Operator,
    KnackToS3Operator,
    SlackNotificationOperator,
)


class CartoPlugin(AirflowPlugin):
    name = 'carto_plugin'
    operators = [S3ToCartoOperator]

class DatabridgePlugin(AirflowPlugin):
    name = 'databridge_plugin'
    operators = [DataBridgeToS3Operator, S3ToDataBridge2Operator]

class KnackPlugin(AirflowPlugin):
    name = 'knack_plugin'
    operators = [KnackToS3Operator]

class SlackPlugin(AirflowPlugin):
    name = 'slack_notify_plugin'
    operators = [SlackNotificationOperator]