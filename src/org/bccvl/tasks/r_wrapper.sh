#!/bin/bash

mkdir -p "${WORKDIR}/R_LIBS"
export R_LIBS="${WORKDIR}/R_LIBS"

exec Rscript --no-save --no-restore $1
