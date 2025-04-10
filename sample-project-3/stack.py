from aws_cdk import (
    Stack,
    aws_s3 as s3,
)

from constructs import Construct

class SampleProject3Stack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str
    ) -> None:
        super().__init__(scope, construct_id)
        
        bucket = s3.Bucket(self, 
                            "SanjayBucket",
                            bucket_name="youtubedemo10apr-sanjaybedwal"
                            )
        
        bucket2 = s3.Bucket(self, 
                            "SanjayBucket2",
                            bucket_name="youtubedemo10apr1-sanjaybedwal"
                            )        
        
        