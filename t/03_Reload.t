use strict;
use warnings;
use Test::More;

require App::SquidArm::Helper;

subtest 'reload' => sub {
    require App::SquidArm::Log;
    ok exists $INC{'App/SquidArm/Log.pm'};
    App::SquidArm::Helper::unload_modules( undef, 'App::SquidArm::Log' );
    ok !exists $INC{'App/SquidArm/Log.pm'} or note explain \%INC;
    require App::SquidArm::Log;
    ok exists $INC{'App/SquidArm/Log.pm'};
};

done_testing;
