import aws_cdk as cdk
from constructs import Construct
from stack import GlueTrigger_cdk


app = cdk.App()
stack = GlueTrigger_cdk(
    scope=app,
    construct_id="glueTrigger-stack-yt"
    )


app.synth()
# Compare this snippet from sample-project-3/stack.py:cdk 