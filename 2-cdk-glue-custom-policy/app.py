import aws_cdk as cdk
from constructs import Construct
from stack import CDKGlueCutomPolicy


app = cdk.App()
stack = CDKGlueCutomPolicy(
    scope=app,
    construct_id="glue-stack-yt-custom-policy"
    )


app.synth()
# Compare this snippet from sample-project-3/stack.py:cdk 