package App::SquidArm::Web;
use strict;
use warnings;
use Dancer qw(:syntax);
use App::SquidArm::Web::API;
use App::SquidArm::DB;
use App::SquidArm::Conf;

#use File::ShareDir qw(dist_dir);

#my $share = dist_dir('App::SquidArm');
my $share = 'share';

set confdir => $share;
set views   => path( $share, 'views' );
set public  => path( $share, 'public' );
set envdir  => path( $share, 'environments' );

Dancer::Config::load();

#print to_dumper(config);

my $conf = App::SquidArm::Conf->new( config => config->{sarm_conf} )->parse;

my $db = App::SquidArm::DB->new(
    db_driver => $conf->tag('db_driver'),
    db_dir    => $conf->tag('db_dir'),
);

hook before => sub {
    var db => $db;
    if ( request->{path_info} =~ m{^/api/} ) {
        content_type 'application/json';
    }
};

sub tmpl($;$) {
    my $tmpl = shift;
    my $p = @_ ? shift : {};
    template $tmpl,
      {
        css => template( "css/$tmpl", {}, { layout => undef } ),
        js  => template( "js/$tmpl",  {}, { layout => undef } ),
        %$p
      };
}

prefix undef;

get '/' => sub {
    tmpl 'index';
};

get '/users' => sub {
    tmpl 'users';
};

get '/users/**' => sub {
    my ($splat) = splat;
    tmpl 'users', { splat => to_dumper($splat) };
};

get '/user/**' => sub {
    my ($splat) = splat;
    tmpl 'users';
};

1
