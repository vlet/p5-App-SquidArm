use strict;
use Test::More;

BEGIN {
    use_ok 'App::SquidArm::Log';
}

subtest 'parse1' => sub {
    my $buffer = do { local $/; <DATA> };
    my $i      = 0;
    my $cnt    = App::SquidArm::Log->new->parser( \$buffer );
    is $cnt, 298, "count 2 ok" or do {
        my $n = index( $buffer, "\n", $cnt );
        print "bad str:\n";
        print substr $buffer, $cnt, ( $n - $cnt );
        print "\n";
    };
};

done_testing;

__DATA__
1402458558.212    375 10.160.1.10 TCP_MISS_ABORTED/000 0 GET http://www.linkedin.com/analytics/? user@HOLDING.COM HIER_DIRECT/91.225.248.80 -
1402459297.507     96 10.160.1.10 TCP_MISS/503 4331 GET http://haproxy.ipv6.1wt.eu/img/ipv6ok.gif user@HOLDING.COM HIER_DIRECT/2001:7a8:363c:2::2 text/html
