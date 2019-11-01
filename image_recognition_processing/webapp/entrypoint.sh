#!/bin/sh

cat >/usr/share/nginx/html/app/config.js <<EOF
"use strict";
exports.CONFIG = {
    DDBAlbumMetadataTable: "${ALBUM_METADATA_TABLE}",
    CognitoIdentityPool: "${COGNITO_IDENTITY_POOL}",
    Region: "${AWS_REGION}",
    DDBImageMetadataTable: "${IMAGE_METADATA_TABLE}",
    S3PhotoRepoBucket: "${PHOTO_REPO_S3_BUCKET}",
    DescribeExecutionLambda: "${DESCRIBE_EXECUTION_FUNCTION_NAME}"
};
EOF

exec "$@"
