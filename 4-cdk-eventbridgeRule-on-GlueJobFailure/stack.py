from aws_cdk import (
    Stack,
    aws_sns as sns,
    aws_events as events,
    aws_events_targets as targets,
)

from constructs import Construct
import config

class EventBridgerule_glueFailure_cdk(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str
    ) -> None:
        super().__init__(scope, construct_id)
        
        env="526844078262"
        sns_topic = sns.Topic.from_topic_arn(
            self, "ImportedSNSTopic",
            f"arn:aws:sns:ap-south-1:{env}:glue-failure-topic-yt"
        )        

        glue_event_pattern = {
        "source": ["aws.glue"],
        "detail_type": ["Glue Job State Change"],
        "detail": 
        {
            "state": ["FAILED"],
            "jobName": ["glue-cdk-job-3","glue-cdk-job-2"]
        }
        }

        # Create an EventBridge Rule for Glue job failure
        glue_failure_rule = events.Rule(
        self, "GlueFailureRule",
        rule_name="glue-failure-rule",
        event_pattern=glue_event_pattern,
        )
        
        glue_failure_rule.add_target(targets.SnsTopic(sns_topic))
        
   