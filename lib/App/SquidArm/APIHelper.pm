package App::SquidArm::APIHelper;
use strict;
use warnings;
use App::SquidArm::Conf;
use App::SquidArm::DB;

my ( $conf, $db );

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

1

