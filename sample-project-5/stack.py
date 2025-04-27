from aws_cdk import (
    Stack,
    aws_iam as iam,
    aws_glue as glue,
    aws_s3_assets as s3_assets,
    
)

from constructs import Construct

class SampleProject5Stack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str
    ) -> None:
        super().__init__(scope, construct_id)
        
        glue_job_role = iam.Role(
            self, "GlueJobIAMRole",
            role_name="GlueJobIAMRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
            ]
        )      

        # add glue script location
        glue_script_loc = s3_assets.Asset(
            self,
            "s3-assets-glue-script",
            path="src/glue/glue_code.py",
        )

        # Define a Glue job (Python shell)
        glue_job = glue.CfnJob(
            self,
            "GlueJob",
            name="glue-cdk-job",
            role=glue_job_role.role_arn,
            command={
                "name": "pythonshell",
                "scriptLocation": glue_script_loc.s3_object_url,
                "pythonVersion": "3.9",
            },
            max_retries=0,
            timeout=3,
        )        

            

        
    
        
        
