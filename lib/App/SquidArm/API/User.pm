package App::SquidArm::API::User;
use strict;
use warnings;
use Raisin::API;
use App::SquidArm::APIHelper;
use Types::Standard qw(Any Int Str);

desc 'user route';
resource user => sub {
    params(
        requires => { name => 'user',  desc => 'user',  type => Str },
        requires => { name => 'year',  desc => 'year',  type => Int },
        requires => { name => 'month', desc => 'month', type => Int },
        optional => { name => 'day',   desc => 'day',   type => Int }
    );
    resource ':user/:year/:month(/:day)?' => sub {
        get sub {
            my $p = shift;
            {
                data => App::SquidArm::APIHelper->db->traf_stat_user_ymd(
                    $p->{user}, $p->{year}, $p->{month}, $p->{day}
                )
            };
          }
      }
};

desc 'usernames route';
resource usernames => sub {
    get sub {
        my $users = App::SquidArm::APIHelper->db->get_users;
        {
            data => { App::SquidArm::APIHelper->usernames->pairs(@$users) }
        };
    }
};

desc 'all users route';
resource users => sub {
    params(
        requires => { name => 'year',  desc => 'year',  type => Int },
        requires => { name => 'month', desc => 'month', type => Int },
        optional => { name => 'day',   desc => 'day',   type => Int }
    );
    resource ':year/:month(/:day)?' => sub {
        get sub {
            my $p = shift;
            {
                data => App::SquidArm::APIHelper->db->traf_stat_all_users_ymd(
                    $p->{year}, $p->{month}, $p->{day} )
            };
          }
      }
};

1
