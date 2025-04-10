import aws_cdk as cdk
from constructs import Construct
from stack import SampleProject3Stack

app = cdk.App()
stack = SampleProject3Stack(
    scope=app,
    construct_id="ap-south-2-project3"
    )
app.synth()
# Compare this snippet from sample-project-3/stack.py:cdk 