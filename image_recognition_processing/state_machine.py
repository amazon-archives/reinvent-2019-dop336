import os.path

from aws_cdk import (
    core,
    aws_dynamodb as dynamodb,
    aws_lambda as lbda,
    aws_iam as iam,
    aws_s3 as s3,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks
)

class IdentifierStateMachine(core.Construct):
    def __init__(self, scope: core.Construct, id: str, photo_repo: s3.Bucket, image_metadata_table: dynamodb.Table) -> None:
        super().__init__(scope, id)

        extract_image_metadata_fn_dir = os.path.join(
            os.path.dirname(__file__), 'extract-image-metadata')
        detect_labels_fn_dir = os.path.join(
            os.path.dirname(__file__), 'rekognition')
        generate_thumbnails_fn_dir = os.path.join(
            os.path.dirname(__file__), 'thumbnail')

        # Function to extract image metadata
        extract_metadata_fn = lbda.Function(
            self, 'ExtractImageMetadata',
            description='Extract image metadata such as format, size, geolocation, etc.',
            code=lbda.AssetCode(extract_image_metadata_fn_dir),
            handler='index.handler',
            runtime=lbda.Runtime.NODEJS_8_10,
            memory_size=1024,
            timeout=core.Duration.seconds(200)
        )
        # Allow extraction function to get photos
        photo_repo.grant_read(extract_metadata_fn)

        # Function to transform image metadata
        transform_metadata_fn = lbda.Function(
            self, 'TransformImageMetadata',
            description='massages JSON of extracted image metadata',
            code=lbda.AssetCode(os.path.join(
                os.path.dirname(__file__), 'transform-metadata')),
            handler='index.handler',
            runtime=lbda.Runtime.NODEJS_8_10,
            memory_size=256,
            timeout=core.Duration.seconds(60)
        )

        # Function to store metadata in database
        store_metadata_fn = lbda.Function(
            self, 'StoreImageMetadata',
            description='Store image metadata into database',
            code=lbda.AssetCode(os.path.join(
                os.path.dirname(__file__), 'store-image-metadata')),
            handler='index.handler',
            runtime=lbda.Runtime.NODEJS_8_10,
            environment={
                'IMAGE_METADATA_DDB_TABLE': image_metadata_table.table_name
            },
            memory_size=256,
            timeout=core.Duration.seconds(60)
        )
        photo_repo.grant_read(store_metadata_fn)
        image_metadata_table.grant_write_data(store_metadata_fn)

        # Invoke Rekognition to detect labels
        detect_labels_fn = lbda.Function(
            self, 'DetectLabels',
            description='Use Amazon Rekognition to detect labels from image',
            code=lbda.AssetCode(detect_labels_fn_dir),
            handler='index.handler',
            runtime=lbda.Runtime.NODEJS_8_10,
            memory_size=256,
            timeout=core.Duration.seconds(60)
        )
        detect_labels_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=['rekognition:DetectLabels'],
                resources=['*']
            )
        )
        photo_repo.grant_read(detect_labels_fn)

        # Generate thumbnails
        generate_thumbnails_fn = lbda.Function(
            self, 'GenerateThumbnails',
            description='Generate thumbnails for images',
            code=lbda.AssetCode(generate_thumbnails_fn_dir),
            handler='index.handler',
            runtime=lbda.Runtime.NODEJS_8_10,
            memory_size=1536,
            timeout=core.Duration.minutes(5)
        )
        photo_repo.grant_read_write(generate_thumbnails_fn)

        # State machine
        not_supported_image_type = sfn.Fail(
            self, 'Unsupported image type',
        )

        extract_metadata_task = sfn.Task(
            self, 'Extract image metadata',
            task=tasks.InvokeFunction(
                extract_metadata_fn),
            input_path='$',
            result_path='$.extractedMetadata',
        ).add_catch(
            errors=['ImageIdentifyError'],
            handler=not_supported_image_type
        ).add_retry(
            errors=['ImageIdentifyError'],
            max_attempts=0
        ).add_retry(
            errors=['States.ALL'],
            interval=core.Duration.seconds(1),
            max_attempts=2,
            backoff_rate=1.5
        )

        transform_metadata_task = sfn.Task(
            self, 'Transform metadata',
            task=tasks.InvokeFunction(transform_metadata_fn),
            input_path='$.extractedMetadata',
            result_path='$.extractedMetadata'
        ).add_retry(
            errors=['States.ALL'],
            interval=core.Duration.seconds(1),
            max_attempts=2,
            backoff_rate=1.5
        )

        detect_labels_task = sfn.Task(
            self, 'Detect labels',
            task=tasks.InvokeFunction(detect_labels_fn),
        ).add_retry(
            errors=['States.ALL'],
            interval=core.Duration.seconds(1),
            max_attempts=2,
            backoff_rate=1.5
        )

        generate_thumbnails_task = sfn.Task(
            self, 'Generate thumbnails',
            task=tasks.InvokeFunction(generate_thumbnails_fn),
        ).add_retry(
            errors=['States.ALL'],
            interval=core.Duration.seconds(1),
            max_attempts=2,
            backoff_rate=1.5
        )

        store_metadata_task = sfn.Task(
            self, 'Store metadata',
            task=tasks.InvokeFunction(store_metadata_fn),
            input_path='$',
            result_path='$.storeResult'
        ).add_retry(
            errors=['States.ALL'],
            interval=core.Duration.seconds(1),
            max_attempts=2,
            backoff_rate=1.5
        )

        parallel_processing = sfn.Parallel(
            self, 'Parallel processing',
            result_path='$.parallelResults'
        )
        parallel_processing.branch(
            detect_labels_task, generate_thumbnails_task
        )

        self.state_machine = sfn.StateMachine(
            self, 'StateMachine',
            definition=extract_metadata_task.next(
                sfn.Choice(
                    self, 'Check image type'
                ).when(
                    sfn.Condition.or_(
                        sfn.Condition.string_equals(
                            '$.extractedMetadata.format', 'JPEG'),
                        sfn.Condition.string_equals(
                            '$.extractedMetadata.format', 'PNG')
                    ),
                    transform_metadata_task.next(
                        parallel_processing).next(store_metadata_task)
                ).otherwise(not_supported_image_type)
            )
        )
