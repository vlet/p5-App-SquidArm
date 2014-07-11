use strict;
use warnings;
use Test::More;

require App::SquidArm::Server;

subtest 'reload' => sub {
    require App::SquidArm::Log;
    ok exists $INC{'App/SquidArm/Log.pm'};
    App::SquidArm::Server::_unload_modules('App::SquidArm::Log');
    ok !exists $INC{'App/SquidArm/Log.pm'} or note explain \%INC;
    require App::SquidArm::Log;
    ok exists $INC{'App/SquidArm/Log.pm'};
};

done_testing;
