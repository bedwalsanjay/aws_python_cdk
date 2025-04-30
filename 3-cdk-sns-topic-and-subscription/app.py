import aws_cdk as cdk
from constructs import Construct
from stack import SNS_cdk


app = cdk.App()
stack = SNS_cdk(
    scope=app,
    construct_id="sns-stack-yt"
    )


app.synth()
# Compare this snippet from sample-project-3/stack.py:cdk 