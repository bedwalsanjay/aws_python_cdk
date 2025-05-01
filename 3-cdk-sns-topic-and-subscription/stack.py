from aws_cdk import (
    Stack,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subs,
    aws_iam as iam,
)

from constructs import Construct
import config

class SNS_cdk(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str
    ) -> None:
        super().__init__(scope, construct_id)
        
        sns_topic = sns.Topic(self, "glue-failure-topic",
                topic_name="glue-failure-topic-yt",
                fifo=False,
                #    master_key=custom_kms_encryption_key
                )
        
        # Grant publish permissions to the EventBridge service
        sns_topic.grant_publish(iam.ServicePrincipal("events.amazonaws.com"))         

        for email_id in config.sns_email_list:
            sns_topic.add_subscription(sns_subs.EmailSubscription(
            email_address=email_id))          
   
