#!/bin/sh -e

export SOURCE_FILES="eventual_tortoise tests"
set -x

black --check --diff $SOURCE_FILES
flake8 $SOURCE_FILES
mypy $SOURCE_FILES
isort --check --diff --project=eventual_tortoise $SOURCE_FILES
