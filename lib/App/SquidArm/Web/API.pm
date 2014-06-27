package App::SquidArm::Web::API;
use strict;
use warnings;
use Dancer qw(:syntax);

prefix '/api';
set serializer => 'JSON';

# Month stat
get '/:year/:month' => sub {
    my $year  = param('year');
    my $month = param('month');

    return undef
      unless $year =~ /^\d{4}$/
      && $month =~ /^\d{1,2}$/
      && $month >= 1
      && $month <= 12;

    my $db = var 'db';
    { data => $db->traf_stat( $year, $month ) };
};

# Month stat
get '/:year/:month/:day' => sub {
    my $year  = param('year');
    my $month = param('month');
    my $day   = param('day');

    return { data => [] }
      unless $year =~ /^\d{4}$/
      && $month    =~ /^\d{1,2}$/
      && $day      =~ /^\d{1,2}$/
      && $month >= 1
      && $month <= 12
      && $day >= 1
      && $day <= 31;

    my $db = var 'db';
    { data => $db->user_traf_stat( $year, $month, $day ) };
};

