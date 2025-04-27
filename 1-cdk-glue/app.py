import aws_cdk as cdk
from constructs import Construct
from stack import CDKGlue


app = cdk.App()
stack = CDKGlue(
    scope=app,
    construct_id="glue-stack-yt"
    )


app.synth()
# Compare this snippet from sample-project-3/stack.py:cdk 