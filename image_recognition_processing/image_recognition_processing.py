import os.path

from aws_cdk import (
    core,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    aws_lambda as lbda,
    aws_lambda_event_sources as event_sources,
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
                name='uploadTIme', type=dynamodb.AttributeType.NUMBER),
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

        # Function to start image processing
        start_execution_fn = lbda.Function(
            self, 'StartExecution',
            description='Triggered by S3 image upload to the repo bucket and start the image processing step function workflow',
            code=lbda.AssetCode(os.path.join(
                os.path.dirname(__file__), 'start-execution')),
            handler='index.handler',
            runtime=lbda.Runtime.NODEJS_8_10,
            environment={
                'IMAGE_METADATA_DDB_TABLE': image_metadata_table.table_name
            },
            memory_size=256
        )
        start_execution_fn.add_event_source(
            event_sources.S3EventSource(
                photo_repo,
                events=[
                    s3.EventType.OBJECT_CREATED
                ]
            ))

        # Function to extract image metadata
        extract_metadata_fn = lbda.Function(
            self, 'ExtractImageMetadata',
            description='Extract image metadata such as format, size, geolocation, etc.',
            code=lbda.AssetCode(os.path.join(
                os.path.dirname(__file__), 'extract-image-metadata')),
            handler='index.handler',
            runtime=lbda.Runtime.NODEJS_8_10,
            memory_size=1024,
            timeout=core.Duration.seconds(200)
        )

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

        # Invoke Rekognition to detect labels
        detect_labels_fn = lbda.Function(
            self, 'DetectLabels',
            description='Use Amazon Rekognition to detect labels from image',
            code=lbda.AssetCode(os.path.join(
                os.path.dirname(__file__), 'rekognition')),
            handler='index.handler',
            runtime=lbda.Runtime.NODEJS_8_10,
            memory_size=256,
            timeout=core.Duration.seconds(60)
        )

        # Generate thumbnails
        generate_thumbnails_fn = lbda.Function(
            self, 'GenerateThumbnails',
            description='Generate thumbnails for images',
            code=lbda.AssetCode(os.path.join(
                os.path.dirname(__file__), 'thumbnail')),
            handler='index.handler',
            runtime=lbda.Runtime.NODEJS_8_10,
            memory_size=1536,
            timeout=core.Duration.minutes(5)
        )

        # State machine
        not_supported_image_type = sfn.Fail(
            self, 'NotSupportedImageType',
            comment='Unsupported image type'
        )

        extract_metadata_task = sfn.Task(
            self, 'ExtractMetadataTask',
            task=tasks.RunLambdaTask(
                extract_metadata_fn),
            comment='Extract Image Metadata',
            result_path='$.extractedMetadata'
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
            self, 'TransformMetadataTask',
            task=tasks.RunLambdaTask(transform_metadata_fn),
            comment='Transform metadata',
            input_path='$.extractedMetadata',
            result_path='$.extractedMetadata'
        ).add_retry(
            errors=['States.ALL'],
            interval=core.Duration.seconds(1),
            max_attempts=2,
            backoff_rate=1.5
        )

        detect_labels_task = sfn.Task(
            self, 'DetectLabelsTask',
            task=tasks.RunLambdaTask(detect_labels_fn),
        ).add_retry(
            errors=['States.ALL'],
            interval=core.Duration.seconds(1),
            max_attempts=2,
            backoff_rate=1.5
        )

        generate_thumbnails_task = sfn.Task(
            self, 'GenerateThumbnailsTask',
            task=tasks.RunLambdaTask(generate_thumbnails_fn),
        ).add_retry(
            errors=['States.ALL'],
            interval=core.Duration.seconds(1),
            max_attempts=2,
            backoff_rate=1.5
        )

        store_metadata_task = sfn.Task(
            self, 'StoreMetadataTask',
            task=tasks.RunLambdaTask(store_metadata_fn),
            result_path='$.storeResult'
        ).add_retry(
            errors=['States.ALL'],
            interval=core.Duration.seconds(1),
            max_attempts=2,
            backoff_rate=1.5
        )

        parallel_processing = sfn.Parallel(
            self, 'ParallelProcessing',
            result_path='$.parallelResults'
        )
        parallel_processing.branch(
            detect_labels_task, generate_thumbnails_task
        )

        sfn.StateMachine(
            self, 'StateMachine',
            definition=extract_metadata_task.next(
                sfn.Choice(
                    self, 'ImageTypeCheck',
                    comment='Check Image Type',
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
