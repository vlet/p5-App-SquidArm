package App::SquidArm::API::Traf;
use strict;
use warnings;
use Raisin::API;
use App::SquidArm::APIHelper;
use Types::Standard qw(Any Int Str);

desc 'traf route';
resource 'traf' => sub {
    params(
        requires => { name => 'year',  desc => 'year',  type => Int },
        requires => { name => 'month', desc => 'month', type => Int },
        optional => { name => 'day',   desc => 'day',   type => Int }
    );
    resource ':year/:month(/:day)?' => sub {
        get sub {
            my $p = shift;
            {
                data => $p->{day}
                ? App::SquidArm::APIHelper->db->traf_stat_all_users_ymd(
                    $p->{year}, $p->{month}, $p->{day} )
                : App::SquidArm::APIHelper->db->traf_stat_ym(
                    $p->{year}, $p->{month}
                )
            };
          }
      }
};

1

