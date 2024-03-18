package App::SquidArm::API;
use strict;
use warnings;
use Raisin::API;

api_format 'json';

mount 'App::SquidArm::API::User';
mount 'App::SquidArm::API::Traf';
mount 'App::SquidArm::API::Site';
