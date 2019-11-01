import setuptools


with open("README.md") as fp:
    long_description = fp.read()


setuptools.setup(
    name="reinvent_dop336_2019",
    version="0.0.1",

    description="An empty CDK Python app",
    long_description=long_description,
    long_description_content_type="text/markdown",

    author="author",

    package_dir={"": "image_recognition_processing"},
    packages=setuptools.find_packages(where="image_recognition_processing"),

    install_requires=[
        "aws-cdk.core",
        "aws-cdk.aws-s3",
        "aws-cdk.aws-dynamodb",
        "aws-cdk.aws-lambda",
        "aws-cdk.aws-stepfunctions",
        "aws-cdk.aws-stepfunctions-tasks",
        "aws-cdk.aws-ecs-patterns",
        "aws-cdk.aws-ecr-assets",
        "aws-cdk.aws-cognito"
    ],

    python_requires=">=3.6",

    classifiers=[
        "Development Status :: 4 - Beta",

        "Intended Audience :: Developers",

        "License :: OSI Approved :: Apache Software License",

        "Programming Language :: JavaScript",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",

        "Topic :: Software Development :: Code Generators",
        "Topic :: Utilities",

        "Typing :: Typed",
    ],
)
