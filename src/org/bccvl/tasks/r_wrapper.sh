#!/bin/bash


# FIXME: these env vars work only in docker env
if [  -z "$R_LIBS" ] ; then
    # if R_LIBS is not set, use workdir for users r libs
    mkdir -p "${WORKDIR}/R_LIBS"
    export R_LIBS="${WORKDIR}/R_LIBS"
fi

exec Rscript --no-save --no-restore $1
