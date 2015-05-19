use strict;
use Test::More;

use_ok $_ for qw(
  App::SquidArm
  App::SquidArm::API
  App::SquidArm::API::Traf
  App::SquidArm::API::User
  App::SquidArm::APIHelper
  App::SquidArm::CacheMgr
  App::SquidArm::Conf
  App::SquidArm::Daemon
  App::SquidArm::DB
  App::SquidArm::DBStore
  App::SquidArm::Helper
  App::SquidArm::LogParser
  App::SquidArm::LogWriter
  App::SquidArm::MemCache
);

done_testing;

