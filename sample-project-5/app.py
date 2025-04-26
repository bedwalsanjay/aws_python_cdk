import aws_cdk as cdk
from constructs import Construct
from stack import SampleProject5Stack


app = cdk.App()
stack = SampleProject5Stack(
    scope=app,
    construct_id="sample-project-5-glue"
    )


app.synth()
# Compare this snippet from sample-project-3/stack.py:cdk 