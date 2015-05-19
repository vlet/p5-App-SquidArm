use strict;
use warnings;
use Test::More;
use DateTime;
use File::Temp qw(tempfile);

subtest 'DST change' => sub {
    plan skip_all => 'not a test at all';

    #my $tz = 'Europe/Moscow';
    my $tz = 'America/New_York';
    for my $year ( 2000 .. 2015 ) {
        for my $month ( 1 .. 12 ) {
            my $dt = DateTime->new(
                year      => $year,
                month     => $month,
                time_zone => $tz
            );
            my $start = $dt->epoch;
            my $end   = $dt->add( months => 1 )->epoch;
            my $diff  = ( $end - $start ) % 86400;
            if ( $diff != 0 ) {
                print "$year $month has $diff\n";
            }
        }
    }
    pass;
};

subtest 'check dst change in traf_stat_ym' => sub {
    require App::SquidArm::DB;
    no warnings qw'once redefine';
    *App::SquidArm::DB::db = sub { };
    *App::SquidArm::DB::_traf_stat_ym = sub {
        return [@_];
    };

    my $db = App::SquidArm::DB->new( tz => 'America/New_York' );
    my $res = $db->traf_stat_ym( 2015, 11 );
    is_deeply $res,
      [
        undef, 'america__new_york', 2015, 11, 1446350400, 1446440400, 1, 90000,
        undef, 'america__new_york', 2015, 11, 1446440400, 1448946000, 2
      ],
      "dst change at 2015-11-01 in NY";
    $res = $db->traf_stat_ym( 2015, 3 );
    is_deeply $res,
      [
        undef,               'america__new_york',
        2015,                3,
        1425186000,          1425790800,
        1,                   undef,
        'america__new_york', 2015,
        3,                   1425790800,
        1425873600,          8,
        82800,               undef,
        'america__new_york', 2015,
        3,                   1425873600,
        1427860800,          9
      ],
      "dst change at 2015-03-08 in NY";
    $res = $db->traf_stat_ym( 2015, 2 );
    is_deeply $res,
      [ undef, 'america__new_york', 2015, 2, 1422766800, 1425186000, 1 ],
      "no dst change";
};

done_testing;
