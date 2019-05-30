from airflow.plugins_manager import AirflowPlugin

from operators import (
    S3ToCartoBatchOperator, S3ToCartoLambdaOperator,
    DataBridgeToS3BatchOperator, DataBridgeToS3LambdaOperator,
    S3ToDataBridge2BatchOperator, S3ToDataBridge2LambdaOperator,
    KnackToS3BatchOperator, KnackToS3LambdaOperator,
    SlackNotificationOperator,
)


class CartoPlugin(AirflowPlugin):
    name = 'carto_plugin'
    operators = [S3ToCartoBatchOperator, S3ToCartoLambdaOperator]

class DatabridgePlugin(AirflowPlugin):
    name = 'databridge_plugin'
    operators = [
        DataBridgeToS3BatchOperator, DataBridgeToS3LambdaOperator, 
        S3ToDataBridge2BatchOperator, S3ToDataBridge2LambdaOperator
    ]

class KnackPlugin(AirflowPlugin):
    name = 'knack_plugin'
    operators = [KnackToS3BatchOperator, KnackToS3LambdaOperator]

class SlackPlugin(AirflowPlugin):
    name = 'slack_notify_plugin'
    operators = [SlackNotificationOperator]
