from airflow.plugins_manager import AirflowPlugin

from operators import (
    AirtableToS3BatchOperator,
    BatchGeocoderOperator,
    S3ToCartoBatchOperator,
    OracleToS3BatchOperator,
    S3ToPostgresBatchOperator,
    KnackToS3BatchOperator,
    SlackNotificationOperator,
)


class AirtablePlugin(AirflowPlugin):
    name = 'airtable_plugin'
    operators = [AirtableToS3BatchOperator]

class BatchGeocoderPlugin(AirflowPlugin):
    name = 'batch_geocoder_plugin'
    operators = [BatchGeocoderOperator]

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
