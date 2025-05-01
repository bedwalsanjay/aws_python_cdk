import aws_cdk as cdk
from constructs import Construct
from stack import EventBridgerule_glueFailure_cdk


app = cdk.App()
stack = EventBridgerule_glueFailure_cdk(
    scope=app,
    construct_id="EventBridgerule-glueFailure-stack-yt"
    )


app.synth()
# Compare this snippet from sample-project-3/stack.py:cdk 