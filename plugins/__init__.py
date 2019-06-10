from airflow.plugins_manager import AirflowPlugin

from operators import (
    S3ToCartoBatchOperator,
    DataBridgeToS3BatchOperator,
    S3ToDataBridge2BatchOperator,
    KnackToS3BatchOperator,
    SlackNotificationOperator,
)


class CartoPlugin(AirflowPlugin):
    name = 'carto_plugin'
    operators = [S3ToCartoBatchOperator]

class DatabridgePlugin(AirflowPlugin):
    name = 'databridge_plugin'
    operators = [DataBridgeToS3BatchOperator, S3ToDataBridge2BatchOperator]

class KnackPlugin(AirflowPlugin):
    name = 'knack_plugin'
    operators = [KnackToS3BatchOperator]

class SlackPlugin(AirflowPlugin):
    name = 'slack_notify_plugin'
    operators = [SlackNotificationOperator]
