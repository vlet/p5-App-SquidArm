use strict;
use warnings;
use Test::More;

BEGIN {
    use_ok 'App::SquidArm::Conf';
}

subtest 'new' => sub {
    new_ok 'App::SquidArm::Conf';
};

subtest 'parser' => sub {
    my $conf = App::SquidArm::Conf->new( config => 'sarm.conf' );
    eval { $conf->parse; };
    is $@, '', 'parse not failed' or return;
    is_deeply $conf->tags,
      {
        'access_log'               => 'db/squid_access.log',
        'db_driver'                => 'sqlite',
        'db_dir'                   => 'db',
        'db_update_interval'       => '2',
        'host'                     => '10.160.1.8',
        'ignore_denied'            => 1,
        'parser_reload_on_restart' => 1,
        'port'                     => '8000',
        'allowed'                  => [ '10.160.0.3', '10.160.0.4' ],
        'log_file'                 => 'sarm.log',
        'log_level'                => 'note',
        'cachemgr'                 => [
            [qw(10.160.0.3 3128 d2ViYWRtaW46cGFzc3dvcmQ=)],
            [qw(10.160.0.4 3128 d2ViYWRtaW46cGFzc3dvcmQ=)]
        ],
      };
};

done_testing
