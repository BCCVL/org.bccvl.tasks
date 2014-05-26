#!/bin/bash
echo "PATH before: $PERL5LIB"
# setup local::lib
eval "$(scl enable perl516 \\"perl -I$HOME/perl5/lib/perl5 -Mlocal::lib\\")"
# add biodivers to path
export PERL5LIB="${PERL5LIB:+${PERL5LIB}:}$HOME/biodiverse/lib"
exec perl "$1"
