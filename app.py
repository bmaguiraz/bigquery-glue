#!/usr/bin/env python3

from aws_cdk import App, Tags

from lib.glue.glue_stack import GlueJobStack


app = App()
gluejob = GlueJobStack(app, "dev-gluejob-stack", description='Gluejob Stack')
Tags.of(gluejob).add("project", "dev-cgluejob-stack")
app.synth()
