from aws_cdk import (
    Stack,
    aws_glue as glue,
)

from constructs import Construct
import config

class GlueTrigger_cdk(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str
    ) -> None:
        super().__init__(scope, construct_id)
        
        # Create a Glue Trigger to run the Glue Job
        glue_trigger = glue.CfnTrigger(
            self, "GlueTrigger",
            name="glue-job-trigger",
            type="SCHEDULED",  # Options: ON_DEMAND, CONDITIONAL, SCHEDULED
            schedule="cron(40 16 * * ? *)",  # (UTC)
            actions=[{
                "jobName": config.glue_job_name
            }],
            start_on_creation=True
        )
        
   