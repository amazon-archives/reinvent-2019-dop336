#!/usr/bin/env python3

from aws_cdk import core

from image_recognition_processing.image_recognition_processing import ImageRecognitionProcessingStack


app = core.App()
ImageRecognitionProcessingStack(app, "reinvent-dop336-2019")

app.synth()
