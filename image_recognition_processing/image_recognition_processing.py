import os.path
import subprocess
from state_machine import IdentifierStateMachine

from aws_cdk import (
    core,
    aws_cognito as cognito,
    aws_dynamodb as dynamodb,
    aws_ecs as ecs,
    aws_ecs_patterns as ecs_patterns,
    aws_ecr_assets as ecr_assets,
    aws_iam as iam,
    aws_lambda as lbda,
    aws_lambda_event_sources as event_sources,
    aws_s3 as s3,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks
)


class ImageRecognitionProcessingStack(core.Stack):
    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Bucket into which stuff will be uploaded
        photo_repo = s3.Bucket(
            self, 'UploadBucket',
            cors=[
                s3.CorsRule(
                    allowed_origins=['*'],
                    allowed_headers=['*'],
                    allowed_methods=[
                        s3.HttpMethods.DELETE,
                        s3.HttpMethods.PUT,
                        s3.HttpMethods.POST,
                        s3.HttpMethods.GET
                    ],
                    exposed_headers=['ETag']
                )
            ]
        )

        # Image metadata table
        image_metadata_table = dynamodb.Table(
            self, 'ImageMetadata',
            partition_key=dynamodb.Attribute(
                name='imageID', type=dynamodb.AttributeType.STRING),
            read_capacity=3,
            write_capacity=3
        )

        image_metadata_table.add_global_secondary_index(
            index_name='albumID-uploadTime-index',
            partition_key=dynamodb.Attribute(
                name='albumID', type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(
                name='uploadTime', type=dynamodb.AttributeType.NUMBER),
            projection_type=dynamodb.ProjectionType.ALL,
            read_capacity=3,
            write_capacity=3
        )

        # Album metadata table
        album_metadata_table = dynamodb.Table(
            self, 'AlbumMetadata',
            partition_key=dynamodb.Attribute(
                name='albumID', type=dynamodb.AttributeType.STRING),
            read_capacity=2,
            write_capacity=1
        )

        album_metadata_table.add_global_secondary_index(
            index_name='userID-creationTime-index',
            partition_key=dynamodb.Attribute(
                name='userID', type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(
                name='creationTime', type=dynamodb.AttributeType.NUMBER),
            projection_type=dynamodb.ProjectionType.ALL,
            read_capacity=2,
            write_capacity=1
        )

        extract_image_metadata_fn_dir = os.path.join(
            os.path.dirname(__file__), 'extract-image-metadata')
        detect_labels_fn_dir = os.path.join(
            os.path.dirname(__file__), 'rekognition')
        generate_thumbnails_fn_dir = os.path.join(
            os.path.dirname(__file__), 'thumbnail')

        for dir in (
            extract_image_metadata_fn_dir,
            detect_labels_fn_dir,
            generate_thumbnails_fn_dir
        ):
            # Prepare npm package
            subprocess.check_call(["npm", "install"], cwd=dir)

        state_machine = IdentifierStateMachine(
            self,
            'StateMachine',
            photo_repo=photo_repo,
            image_metadata_table=image_metadata_table
        ).state_machine

        # Function to start image processing
        start_execution_fn = lbda.Function(
            self, 'StartExecution',
            description='Triggered by S3 image upload to the repo bucket and start the image processing step function workflow',
            code=lbda.AssetCode(os.path.join(
                os.path.dirname(__file__), 'start-execution')),
            handler='index.handler',
            runtime=lbda.Runtime.NODEJS_8_10,
            environment={
                'STATE_MACHINE_ARN': state_machine.state_machine_arn,
                'IMAGE_METADATA_DDB_TABLE': image_metadata_table.table_name
            },
            memory_size=256
        )

        # Trigger process when the bucket is written to
        start_execution_fn.add_event_source(
            event_sources.S3EventSource(
                photo_repo,
                events=[
                    s3.EventType.OBJECT_CREATED
                ]
            ))
        # Allow start-processing function to write to image metadata table
        image_metadata_table.grant_write_data(start_execution_fn)

        # Allow start-processing function to invoke state machine
        state_machine.grant_start_execution(start_execution_fn)

        describe_execution_function = lbda.Function(
            self, 'DescribeExecutionFunction',
            code=lbda.AssetCode(os.path.join(
                os.path.join(os.path.dirname(__file__),
                             'state-machine-describe-execution')
            )),
            handler='index.handler',
            runtime=lbda.Runtime.NODEJS_8_10,
            memory_size=1024,
            timeout=core.Duration.seconds(200)
        )
        describe_execution_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=['states:DescribeExecution'],
                resources=[
                    self.format_arn(
                        service='states',
                        resource='execution:' + state_machine.state_machine_name,
                        resource_name='*',
                        sep=':'
                    )
                ]
            )
        )

        identity_pool_role = iam.Role(
            self, 'IdentityPoolRole',
            assumed_by=iam.FederatedPrincipal(
                'cognito-identity.amazonaws.com',
                {},
                'sts:AssumeRoleWithWebIdentity'
            )
        )

        identity_pool_role.add_to_policy(
            iam.PolicyStatement(
                actions=['states:DescribeExecution'],
                resources=[
                    self.format_arn(
                        service='states',
                        resource='execution:' + state_machine.state_machine_name,
                        resource_name='*',
                        sep=':'
                    )
                ]
            )
        )

        photo_repo.grant_read_write(identity_pool_role)
        album_metadata_table.grant_read_write_data(identity_pool_role)
        image_metadata_table.grant_read_write_data(identity_pool_role)
        describe_execution_function.grant_invoke(identity_pool_role)

        identity_pool = cognito.CfnIdentityPool(
            self, 'IdentityPool',
            allow_unauthenticated_identities=True
        )
        core.CfnOutput(self, 'identity_pool_id', value=identity_pool.ref)
        cognito.CfnIdentityPoolRoleAttachment(
            self, 'IdentityPoolRoleAttachment',
            identity_pool_id=identity_pool.ref,
            roles={
                'authenticated': identity_pool_role.role_arn,
                'unauthenticated': identity_pool_role.role_arn
            }
        )

        # Our web app
        app = ecs_patterns.ApplicationLoadBalancedFargateService(
            self, 'WebService',
            assign_public_ip=True,
            cpu=256,
            memory_limit_mib=512,
            desired_count=2,
            task_image_options=ecs_patterns.ApplicationLoadBalancedTaskImageOptions(
                image=ecs.ContainerImage.from_asset(
                    os.path.join(os.path.dirname(__file__), 'webapp')
                ),
                container_port=80,
                environment={
                    'ALBUM_METADATA_TABLE': album_metadata_table.table_name,
                    'COGNITO_IDENTITY_POOL': identity_pool.ref,
                    'IMAGE_METADATA_TABLE': image_metadata_table.table_name,
                    'PHOTO_REPO_S3_BUCKET': photo_repo.bucket_name,
                    'DESCRIBE_EXECUTION_FUNCTION_NAME': describe_execution_function.function_name
                }
            )
        )

        app.task_definition.add_to_task_role_policy(
            iam.PolicyStatement(
                actions=['lambda:InvokeFunction'],
                resources=[start_execution_fn.function_arn]
            )
        )
