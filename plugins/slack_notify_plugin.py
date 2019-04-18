from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.plugins_manager import AirflowPlugin


slack_webhook_token = BaseHook.get_connection('slack').password

class SlackNotificationOperator(BaseOperator):
    """Pings a specified channel with a link to a log on success or failure."""
    
    @staticmethod
    def failed(context):
         slack_msg = """
            :red_circle: Task Failed.
            *Task*: {task}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *Log Url*: {log_url}
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
         )
         failed_alert = SlackWebhookOperator(
            task_id='slack_test',
            http_conn_id='slack',
            webhook_token=slack_webhook_token,
            message=slack_msg,
            username='airflow',
            dag=context.get('dag'),
         )
         return failed_alert.execute(context=context)

    @staticmethod
    def success(context):
        slack_msg = """
            :green_heart: Task Succeeded.
            *Task*: {task}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *Log Url*: {log_url}
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
        )
        succeeded_alert = SlackWebhookOperator(
            task_id='slack_test',
            http_conn_id='slack',
            webhook_token=slack_webhook_token,
            message=slack_msg,
            username='airflow',
            dag=context.get('dag'),
        )
        return succeeded_alert.execute(context=context)

class SlackNotifyPlugin(AirflowPlugin):
    name = 'slack_notify_plugin'
    operators = [SlackNotificationOperator]
