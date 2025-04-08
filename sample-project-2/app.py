import aws_cdk as cdk
from constructs import Construct
from stack1 import SampleProject1Stack
import config


app = cdk.App()
stack = SampleProject1Stack(
    scope=app,
    construct_id="SampleProject2Stack",
    account_id=config.ACCOUNT_ID,
    region=config.REGION,
    )
app.synth()


# glue_cdk_stack = GlueStackClass(
#     scope=app,
#     construct_id="cdk-glue-demo",
#     account_id=config.ACCOUNT_ID,
#     region=config.REGION,
#     # env=["dev"],
#     # synthesizer=stack_synthesizer,
# )