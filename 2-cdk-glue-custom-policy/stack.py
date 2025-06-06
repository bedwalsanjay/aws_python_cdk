from aws_cdk import (
    Stack,
    aws_iam as iam,
    aws_glue as glue,
    aws_s3_assets as s3_assets,
    
)

from constructs import Construct

class CDKGlueCutomPolicy(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str
    ) -> None:
        super().__init__(scope, construct_id)
        
        glue_job_role = iam.Role(
            self, "GlueJobIAMRole3",
            role_name="GlueJobIAMRole3",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
                # iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
            ]
        )

        # Create a separate IAM policy statement with restricted S3 permissions
        restricted_s3_access = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["s3:GetObject"],
            resources=["arn:aws:s3:::cdk-hnb659fds-assets-526844078262-ap-south-1/*"]
        )

        # Attach the policy statement to the IAM role
        glue_job_role.add_to_policy(restricted_s3_access)        

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
            name="glue-cdk-job-3",
            role=glue_job_role.role_arn,
            command={
                "name": "pythonshell",
                "scriptLocation": glue_script_loc.s3_object_url,
                "pythonVersion": "3.9",
            },
            default_arguments={
                "--fname": "sanjay",
                "--lname": "bedwal",
            },             
            max_retries=0,
            timeout=3,
        )        

            

        
    
        
        
