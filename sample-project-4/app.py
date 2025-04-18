import aws_cdk as cdk
from constructs import Construct
from stack import SampleProject4Stack

app = cdk.App()
stack = SampleProject4Stack(
    scope=app,
    construct_id="sample-project-4"
    )
app.synth()
# Compare this snippet from sample-project-3/stack.py:cdk 