# flake8: noqa: E501
# pylint: disable=import-error
from constructs import Construct
from aws_cdk import (
    Stack,
    RemovalPolicy,
    aws_s3 as s3,
    aws_iam as iam,
    aws_ssm as ssm,
    aws_sns as sns,
    aws_kms as kms,
    aws_glue as glue,
    aws_events as events,
    aws_dynamodb as dynamodb,
    aws_stepfunctions as sfn,
    aws_s3_assets as s3_assets,
    aws_events_targets as targets,
    aws_sns_subscriptions as sns_subs,
    aws_stepfunctions_tasks as tasks,
    aws_s3_deployment as s3_deploy,
)
import src.config as config
import src.glue_config as glue_config
from src.athena.run_athena_sqls import run_athena_sqls
import boto3
import os


class youtubeStack(Stack):

    def __init__(
            self,
            scope: Construct,
            construct_id: str,
            account_id,
            env,
            **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Iam Role
        iam_role_youtube = iam.Role(
            self,
            'iam-role-youtube',
            role_name='delegate-admin-youtube-role',
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal('glue.amazonaws.com'),
                iam.ServicePrincipal("lambda.amazonaws.com"),
                iam.ServicePrincipal("events.amazonaws.com"),
                iam.ServicePrincipal("states.ap-south-1.amazonaws.com"),
                iam.ServicePrincipal("dynamodb.amazonaws.com")
            ),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AmazonS3FullAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AWSGlueConsoleFullAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'CloudWatchFullAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AWSCloudFormationFullAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AmazonAthenaFullAccess'),
                # iam.ManagedPolicy.from_aws_managed_policy_name('AWSLambda_FullAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AmazonDynamoDBFullAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AmazonSNSFullAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AWSStepFunctionsFullAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AmazonEventBridgeFullAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'SecretsManagerReadWrite')         # added for redshift
            ]
        )

        iam_role_youtube.attach_inline_policy(iam.Policy(
            self,
            'delegate-admin-youtube-kms-inline-policy',
            policy_name='delegate-admin-youtube-kms-inline-policy',
            statements=[
                iam.PolicyStatement(
                    actions=[
                        'kms:Decrypt',
                        'kms:DescribeKey',
                        'kms:Encrypt',
                        'kms:GenerateDataKey'
                    ],
                    resources=["*"]
                )
            ]
        ))

        iam_role_youtube.attach_inline_policy(
            iam.Policy(
                self,
                'delegate-admin-youtube-dynamodb-inline-policy',
                policy_name='delegate-admin-youtube-dynamodb-inline-policy',
                statements=[
                    iam.PolicyStatement(
                        actions=[
                            "dynamodb:BatchGetItem",
                            "dynamodb:BatchWriteItem",
                            "dynamodb:ConditionCheckItem",
                            "dynamodb:PutItem",
                            "dynamodb:DescribeTable",
                            "dynamodb:DeleteItem",
                            "dynamodb:GetItem",
                            "dynamodb:Scan",
                            "dynamodb:Query",
                            "dynamodb:UpdateItem"],
                        resources=[
                            "arn:aws:dynamodb:ap-south-1:" +
                            account_id +
                            ":table/" +
                            config.ddb_audit_table_nm])]))

        iam_role_youtube.attach_inline_policy(
            iam.Policy(
                self,
                'delegate-admin-youtube-step-function-inline-policy',
                policy_name='delegate-admin-youtube-step-function-inline-policy',
                statements=[
                    iam.PolicyStatement(
                        actions=["states:StartExecution"],
                        resources=["*"])]))

        iam_role_youtube.attach_inline_policy(
            iam.Policy(
                self,
                'delegate-admin-youtube-ec2-inline-policy',
                policy_name='delegate-admin-youtube-ec2-inline-policy',
                statements=[
                    iam.PolicyStatement(
                        actions=[
                            "ec2:CreateNetworkInterface",
                            "ec2:CreateTags",
                            "ec2:DeleteNetworkInterface"],
                        resources=["*"]
                    )
                ]
            )
        )

        iam_role_youtube.attach_inline_policy(
            iam.Policy(
                self,
                'delegate-admin-youtube-glue-inline-policy',
                policy_name='delegate-admin-youtube-glue-inline-policy',
                statements=[
                    iam.PolicyStatement(
                        actions=["glue:CreateDatabase", "glue:TagResource"],
                        resources=["*"])]))

        ssm.StringParameter(
            self, 'iam-role-youtube-arn',
            parameter_name='/cdk/business_unit/youtube/iam_role_arn',
            string_value=iam_role_youtube.role_arn
        )

        # DynamoDb table
        dynamodb_audit_table = dynamodb.Table(
            self,
            'dynamodb-youtube-audit-table',
            table_name=config.ddb_audit_table_nm,
            partition_key=dynamodb.Attribute(
                name='file_s3_uri',
                type=dynamodb.AttributeType.STRING),
            removal_policy=RemovalPolicy.DESTROY,
            encryption=dynamodb.TableEncryption.AWS_MANAGED)

        # get aws managed key
        aws_managed_sns_key = kms.Key.from_key_arn(
            self, "imported-sns-key", key_arn=config.sns_kms_key_arn)

        # SNS
        sns_youtube = sns.Topic(
            self, 'sns-topic-youtube',
            display_name='topic_youtube',
            topic_name="topic_youtube",
            master_key=aws_managed_sns_key
        )
        for email_id in config.sns_email_list:
            sns_youtube.add_subscription(
                sns_subs.EmailSubscription(
                    email_address=email_id))

        if self.check_sec_config(config.glue_security_config):
            glue.CfnSecurityConfiguration(
                self,
                "glue_sec_config",
                encryption_configuration=glue.CfnSecurityConfiguration
                .EncryptionConfigurationProperty(
                    s3_encryptions=[
                        glue.CfnSecurityConfiguration.S3EncryptionProperty(
                            kms_key_arn=config.s3_kms_key_arn,
                            s3_encryption_mode="SSE-KMS")]),
                name=config.glue_security_config)

        # Glue job
        glue_env_var = {
            "--strd_bucket_nm"       : config.s3_structured_bucket_nm,
            "--audit_table_nm"       : dynamodb_audit_table.table_name,
            "--sns_topic_arn"        : sns_youtube.topic_arn,
            "--git_repo_name"        : config.git_repo_name,
            "--extra-py-files"       : f"{config.glue_extra_py_path},{config.common_extra_py_path}",
            "--src_file_prefix"      : config.s3_path_prefix,
            "--src_bucket_nm"        : config.s3_raw_bucket_nm,
            "--secret_name"          : config.secret_name,
            "--schema_file_s3_bucket": config.project_bucket_nm,
            "--schema_file_s3_key"   : "utility/youtube/schema.yaml",
            '--enable-auto-scaling'  : 'true',
			'--redshift_iam_role_arn':config.redshift_iam_role_arn,
			'----additional-python-modules'  : 'pg8000'
			
        }

        # add glue script location
        youtube_count_validation_loc = s3_assets.Asset(
            self,
            's3-assets-youtube-count-validation',
            path='src/glue/youtube_count_validation.py',
        )

        # define glue job for count validation
        youtube_count_validation_job = glue.CfnJob(
            self,
            'glue-job-youtube-count-validation',
            name="youtube_count_validation_job",
            role=iam_role_youtube.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name='glueetl',
                python_version='3',
                script_location=youtube_count_validation_loc.s3_object_url),
            default_arguments=glue_env_var,
            description='Glue Job to process and load raw data to structure',
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=100),
            max_retries=config.max_retries,
            timeout=120,
            glue_version='4.0',
            number_of_workers=2,
            security_configuration=config.glue_security_config,
            worker_type='G.1X',
            connections=glue.CfnJob.ConnectionsListProperty(
                connections=['Redshift SG']
            )
        )

        self.glue_scheduler = glue.CfnTrigger(
            self,
            "glue-job-scheduler",
            actions=[glue.CfnTrigger.ActionProperty(
                arguments=glue_env_var,
                job_name=youtube_count_validation_job.name,
                timeout=120,
            )],
            type="SCHEDULED",
            description="add glue job scheduled trigger",
            schedule="cron(00 01 * * ? *)",
            start_on_creation=True
        )

        # step function definition
        # create DynamodbPutItem
        dynamodb_audit_table_task = tasks.DynamoPutItem(
            self, "task-dynamodb-audit-table",
            table=dynamodb_audit_table,
            item={
                "file_s3_uri": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("States.Format('s3://{}/{}',$.bucket.name, $.object.key)")
                ),
                "application": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("States.ArrayGetItem(States.StringSplit($.object.key, '/'), 0)")
                ),
                "data_set": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("States.ArrayGetItem(States.StringSplit($.object.key, '/'), 3)")
                ),
                "file_date": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("States.ArrayGetItem(States.StringSplit($.object.key, '/'), 4)")
                ),
                "src_file_nm": tasks.DynamoAttributeValue.from_string(
                    sfn.JsonPath.string_at("States.ArrayGetItem(States.StringSplit($.object.key, '/'), 5)")
                ),
                "date_processed": tasks.DynamoAttributeValue.from_string(""),
                "status"        : tasks.DynamoAttributeValue.from_string("processed at step-function"),
                "job_run_id"    : tasks.DynamoAttributeValue.from_string(""),
                "stage"         : tasks.DynamoAttributeValue.from_string("raw")
            },
            input_path="$.detail",
            result_path=sfn.JsonPath.DISCARD
        )
        # To modify response json
        pass_task_1 = sfn.Pass(
            self, "task-pass-1",
            input_path="$.detail",
            parameters={
                "src_bucket_nm.$": "$.bucket.name",
                "src_file_key.$" : "$.object.key",
                "dataset.$"      : "States.ArrayGetItem(States.StringSplit($.object.key, '/'), 3)"
            }
        )

        # choice state
        choice_task_1 = sfn.Choice(self, "task-choice-1")
        # create SNSPublish
        sns_youtube_task = tasks.SnsPublish(
            self, "task-sns-youtube",
            topic   = sns_youtube,
            subject = "youtube Lambda file trigger failed",
            message = sfn.TaskInput.from_json_path_at(
                "States.Format('Lambda trigger failed, received file type: {}.\
                 Expected file type: item1, item2, item3, item4, item5.', $.dataset)"),
        )

        # deploy glue jobs in loop STARTS
        for table_item in glue_config.glue_job_metadata:
            file_type           = table_item["file_type"]
            worker_type         = table_item["worker_type"]
            number_of_workers   = table_item["number_of_workers"]
            redshift_table_name = table_item["redshift_table_name"]
            job_type            = table_item["job_type"]
            cron_schedule       = table_item.get("cron_schedule", "")

            # set glue job path
            youtube_common_script_loc = s3_assets.Asset(
                self,
                f's3_assets_{file_type}',
                path='src/glue/youtube_ingestion.py',
            )

            # create glue job
            youtube_glue_job = glue.CfnJob(
                self,
                f'glue_job_{file_type}',
                name                   = f'youtube_{file_type}_job',
                role                   = iam_role_youtube.role_arn,
                description            = f'Glue Job to process and load {file_type} data from raw to structured and redshift table',
                execution_property     = glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs = 10),
                max_retries            = config.max_retries,
                timeout                = 120,
                glue_version           = '4.0',
                worker_type            = worker_type,
                number_of_workers      = number_of_workers,
                security_configuration = config.glue_security_config,
                connections            = glue.CfnJob.ConnectionsListProperty(connections=['Redshift SG']),
                command                = glue.CfnJob.JobCommandProperty(
                    name               = 'glueetl',
                    python_version     = '3',
                    script_location    = youtube_common_script_loc.s3_object_url
                    ),
                default_arguments      = {
                    **glue_env_var,
                    '--redshift_table_name': redshift_table_name
                    },
                )

            if job_type == 'event_based':
                # create step function task to run glue job
                youtube_job_task    = tasks.GlueStartJobRun(
                    self,
                    f"task_{file_type}_glue_job",
                    glue_job_name  = youtube_glue_job.name,
                    arguments      = sfn.TaskInput.from_object({
                            "--src_bucket_nm.$": "$.src_bucket_nm",
                            "--src_file_key.$" : "$.src_file_key"
                            })
                    )

                # add choice for triggering the glue job task
                choice_task_1.when(sfn.Condition.string_equals("$.dataset",file_type),
                                   youtube_job_task)

            else:
                # create schedule for runnnng SCHEDULED glue job
                self.glue_scheduler = glue.CfnTrigger(
                    self,
                    f"glue_job_{file_type}_scheduler",
                    type              = "SCHEDULED",
                    description       = f"Glue job scheduled trigger for {file_type}",
                    schedule          = cron_schedule,
                    start_on_creation = True,
                    actions           = [glue.CfnTrigger.ActionProperty(
                                        job_name  = youtube_glue_job.name,
                                        timeout   = 120,
                                        arguments = {
                                            **glue_env_var,
                                            '--redshift_table_name': redshift_table_name
                                        })]
                )
        # deploy glue jobs in loop ENDS

        choice_task_1.otherwise(sns_youtube_task)
        definition = dynamodb_audit_table_task.next(pass_task_1.next(choice_task_1))

        # create step function
        # log_group = logs.LogGroup(self, "/aws/vendedlogs/states/sfn_youtube")
        sfn_machine = sfn.StateMachine(
            self, "state-machine-youtube",
            state_machine_name=config.sfn_nm,
            definition=definition,
            # role=iam_role_youtube,
            # logs=sfn.LogOptions(
            #  destination=log_group,
            #  level=sfn.LogLevel.ALL
            # )
        )

        # Get source bucket
        raw_bucket_obj = s3.Bucket.from_bucket_name(
            self, 's3-youtube-raw-bucket', bucket_name=config.s3_raw_bucket_nm)
        raw_bucket_obj.enable_event_bridge_notification()

        # Add source event trigger to events rule
        rule_s3 = events.Rule(
            self, "rule-s3-event",
            rule_name=config.event_rule_nm,
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [config.s3_raw_bucket_nm]},
                    "object": {"key": [{"prefix": config.s3_path_prefix}]}
                }
            )
        )

        rule_s3.add_target(
            targets.SfnStateMachine(
                sfn_machine,
                role=iam_role_youtube))

        cicd = os.environ.get("CICD_RUN")
        if not cicd:
            # create athena tables on S3 location
            run_athena_sqls(
                sql_file_prefix='./src/athena/',
                athena_result_loc_s3="s3://" +
                config.athena_local_bt_nm +
                "/youtube_temp_files/")

        glue_utils_zipfile = "glue_utils_zipfile.zip"
        if not os.path.exists(glue_utils_zipfile):
            raise FileNotFoundError(
                f"Schema zipfile {glue_utils_zipfile} does not exist"
            )

        schema_bucket = s3.Bucket.from_bucket_name(
            self, "UtilsBucketByName", config.project_bucket_nm)

        s3_deploy.BucketDeployment(
            self, "UtilsUpload",
            destination_bucket=schema_bucket,
            destination_key_prefix="utility/youtube",
            server_side_encryption=s3_deploy.ServerSideEncryption.AWS_KMS,
            sources=[s3_deploy.Source.asset(f"./{glue_utils_zipfile}")],
            role=iam_role_youtube
        )

    def check_sec_config(self, config_name):
        glue_resource = boto3.client('glue', region_name='ap-south-1')
        missing = False
        try:
            glue_resource.get_security_configuration(Name=config_name)
        except BaseException:
            missing = True

        return missing
