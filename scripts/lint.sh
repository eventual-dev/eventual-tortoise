#!/bin/sh -e

export SOURCE_FILES="eventual_tortoise tests"
set -x

autoflake --in-place --recursive $SOURCE_FILES
isort --project=eventual_tortoise $SOURCE_FILES
black $SOURCE_FILES
