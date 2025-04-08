from aws_cdk import (
    Stack,
    aws_s3 as s3,
)

from constructs import Construct
import config
import aws_cdk as cdk


class SampleProject1Stack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        account_id: str,
        region: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        bucket = s3.Bucket(self, 
                            "SanjayBucket",
                            bucket_name="xyz123456",
                            versioned=False,
                            removal_policy=cdk.RemovalPolicy.DESTROY)        
        