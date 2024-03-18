package App::SquidArm::APIHelper;
use strict;
use warnings;
use App::SquidArm::Conf;
use App::SquidArm::DB;
use App::SquidArm::Usernames;

my ( $conf, $db, $usernames );

sub conf {
    $conf ||=
      App::SquidArm::Conf->new( config => $ENV{SARM_CONF} || 'sarm.conf' )
      ->parse->tags;
}

sub db {
    $db ||= App::SquidArm::DB->new(
        db_driver => __PACKAGE__->conf->{db_driver},
        db_dir    => __PACKAGE__->conf->{db_dir},
        tz        => __PACKAGE__->conf->{tz},
    );
}

sub usernames {
    $usernames ||= App::SquidArm::Usernames->new(
        source => __PACKAGE__->conf->{usernames_source}
    );
}

1

