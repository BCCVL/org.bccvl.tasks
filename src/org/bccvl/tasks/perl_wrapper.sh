#!/bin/bash
# check if perl is from scl
SCL=$(which scl 2>/dev/null)
if [ -n "$SCL" ] ; then
    # we have SCL
    if $SCL -l | grep -q perl516 ; then
        # and perl516
        # setup local::lib
        eval "$(scl enable perl516 \\"perl -I$HOME/perl5/lib/perl5 -Mlocal::lib\\")"
        # add biodiverse to path
        export PERL5LIB="${PERL5LIB:+${PERL5LIB}:}$HOME/biodiverse/lib"
        # print out installed packages and versions
        #scl enable pert516 "cpan -l"
        # replace this process with perl
        exec scl enable perl516 "perl '$1'"
    fi
fi
# no scl or perl516 in scl
# add biodiverse to path
export PERL5LIB="${PERL5LIB:+${PERL5LIB}:}/opt/biodiverse-r1.0/lib"
exec perl "$1"
