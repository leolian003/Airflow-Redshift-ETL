from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 
                 # redshift_conn_id=your-connection-name
                 aws_credentials_id,
                 redshift_conn_id,
                 source_table,
                 source_format,
                 region,
                 s3_key,
                 s3_bucket,
                 provide_context,
                 backfill_date,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id  = redshift_conn_id
        self.source_table = source_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.format = source_format
        self.backfill_date = backfill_date
        


    def execute(self, context):
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        db = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        base_string = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            REGION '{}'
            TIMEFORMAT as 'epochmillisecs'
            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
            {} 'auto' 
            {}
        """
        
        self.log.info("Initiating destination table")
        db.run("DELETE FROM {}".format(self.source_table))        
        s3_path = "s3://{}".format(self.s3_bucket)
        
        #Backfill 
        if self.backfill_date: 
            year = self.backfill_date.strftime("%Y")
            month = self.backfill_date.strftime("%m")
            day = self.backfill_date.strftime("%d")
            s3_path = '/'.join([s3_path, str(year), str(month), str(day)])
        s3_path = s3_path + '/' + self.s3_key

        additional=""
        if self.format == 'csv':
            additional = " DELIMETER ',' IGNOREHEADER 1 "

        formatted_sql = base_string.format(
            self.source_table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.format,
            additional
        )
        db.run(formatted_sql)
        self.log.info(f"Success: {self.task_id}")
        





