from airflow.plugins_manager import AirflowPlugin

from operators import (
    S3ToCartoBatchOperator,
    OracleToS3BatchOperator,
    S3ToPostgresBatchOperator,
    KnackToS3BatchOperator,
    SlackNotificationOperator,
)


class CartoPlugin(AirflowPlugin):
    name = 'carto_plugin'
    operators = [S3ToCartoBatchOperator]

class OracleToS3BatchPlugin(AirflowPlugin):
    name = 'oracle_to_s3_batch_plugin'
    operators = [OracleToS3BatchOperator]

class KnackPlugin(AirflowPlugin):
    name = 'knack_plugin'
    operators = [KnackToS3BatchOperator]

class S3ToPostgresBatchPlugin(AirflowPlugin):
    name = 's3_to_postgres_batch_plugin'
    operators = [S3ToPostgresBatchOperator]

class SlackPlugin(AirflowPlugin):
    name = 'slack_notify_plugin'
    operators = [SlackNotificationOperator]
