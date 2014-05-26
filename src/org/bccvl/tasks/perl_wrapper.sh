#!/bin/bash
# setup local::lib
eval "$(scl enable perl516 \\"perl -I$HOME/perl5/lib/perl5 -Mlocal::lib\\")"
# add biodiverse to path
export PERL5LIB="${PERL5LIB:+${PERL5LIB}:}$HOME/biodiverse/lib"
exec scl enable perl516 "perl '$1'"
