from aws_cdk import (
    Stack,
    aws_iam as iam,
    
)

from constructs import Construct

class SampleProject4Stack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str
    ) -> None:
        super().__init__(scope, construct_id)
        
        glue_job_role = iam.Role(
            self, "GlueJobIAMRole1",
            role_name="GlueJobIAMRole1",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ]
        )        

        glue_job_role = iam.Role(
            self, "GlueJobIAMRole2",
            role_name="GlueJobIAMRole2",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ]
        ) 