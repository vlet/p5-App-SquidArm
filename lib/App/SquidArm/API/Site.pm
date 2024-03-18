package App::SquidArm::API::Site;
use strict;
use warnings;
use Raisin::API;
use App::SquidArm::APIHelper;
use Types::Standard qw(Any Int Str);

desc 'site route';
resource site => sub {
    params(
        requires => { name => 'site',  desc => 'site',  type => Str },
        requires => { name => 'year',  desc => 'year',  type => Int },
        requires => { name => 'month', desc => 'month', type => Int },
        optional => { name => 'day',   desc => 'day',   type => Int }
    );
    resource ':site/:year/:month(/:day)?' => sub {
        get sub {
            my $p = shift;
            {
                data => App::SquidArm::APIHelper->db->site_stat_ymd(
                    $p->{site}, $p->{year}, $p->{month}, $p->{day}
                )
            };
          }
      }
};

desc 'all sites route';
resource sites => sub {
    params(
        requires => { name => 'year',  desc => 'year',  type => Int },
        requires => { name => 'month', desc => 'month', type => Int },
        optional => { name => 'day',   desc => 'day',   type => Int }
    );
    resource ':year/:month(/:day)?' => sub {
        get sub {
            my $p = shift;
            {
                data => App::SquidArm::APIHelper->db->sites_stat_ymd(
                    $p->{year}, $p->{month}, $p->{day} )
            };
          }
      }
};

1
