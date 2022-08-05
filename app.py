#!/usr/bin/env python3

from aws_cdk import App, Tags

from lib.glue.glue_stack import GlueJobStack


app = App()
gluejob = GlueJobStack(app, "dev-gcp-gluejob-stack", description='Gluejob Stack for GCP')
Tags.of(gluejob).add("project", "dev-gluejob-stack")
app.synth()
