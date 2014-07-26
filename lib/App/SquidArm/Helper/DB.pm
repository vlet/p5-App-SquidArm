package App::SquidArm::Helper::DB;
use strict;
use warnings;
use parent qw(App::SquidArm::Helper);
use AnyEvent;
use AnyEvent::Handle;

sub end {
    my $self = shift;
    $self->{db_pipe}->destroy
      if $self->{db_pipe};
    undef $self->{m};
    undef $self->{db};
}

sub begin {
    my ( $self, $db_rd ) = @_;

    $self->unload_modules( 'App::SquidArm::DB', 'App::SquidArm::Log' )
      if $self->conf('parser_reload_on_restart');
    require App::SquidArm::DB;
    require App::SquidArm::Log;

    $self->{parser} =
      App::SquidArm::Log->new( ignore_denied => $self->conf('ignore_denied') );
    my $db = $self->{db} = App::SquidArm::DB->new(
        db_driver => $self->conf('db_driver'),
        db_dir    => $self->conf('db_dir'),
    );
    $db->create_tables;
    AE::log debug => "init db";

    $self->{hosts_cache} = { map { $_ => 1 } @{ $db->get_hosts } };
    $self->{users_cache} = { map { $_ => 1 } @{ $db->get_users } };
    $self->{records} ||= [];
    $self->{stats}   ||= [];

    my $interval = $self->conf('db_update_interval') || 1;
    my $w = AE::cv;
    my ( $h, $tm );

    my $hup = AE::signal HUP => sub {
        AE::log warn => "got HUP";
        eval { ref $tm eq "ARRAY" ? $tm->[1]->() : $tm->cb->(); };
        undef $tm;
        $w->send();
    };

    my $term = AE::signal TERM => sub {
        AE::log warn => "got TERM";
        eval { ref $tm eq "ARRAY" ? $tm->[1]->() : $tm->cb->(); };
        undef $tm;
        $w->send(1);
    };

    my $int = AE::signal INT => sub {
        AE::log error => "got INT. Ignoring...";
    };

    $h = $self->{db_pipe} = AnyEvent::Handle->new(
        fh      => $db_rd,
        on_read => sub {
            $h->push_read(
                storable => sub {
                    if ( defined $_[1] && ref $_[1] eq "ARRAY" ) {
                        my $type = shift @{ $_[1] };
                        if ( $type eq 'records' ) {
                            push @{ $self->{records} }, @{ $_[1] };
                        }
                        elsif ( $type eq 'stats' ) {
                            push @{ $self->{stats} }, @{ $_[1] };
                        }
                        else {
                            AE::log error => "unknown data type $type";
                        }
                    }
                    else {
                        AE::log error => "got malformed records";
                    }
                }
            );
        },
        on_error => sub {
            AE::log error => "got db_pipe error: $!";
            eval { ref $tm eq "ARRAY" ? $tm->[1]->() : $tm->cb->(); };
            undef $tm;
            $w->send();
        },
    );

    my $create_timer;
    $tm = (
        $create_timer = sub {
            return AE::timer $interval, 0, sub {
                eval { $self->update_db(); };
                if ($@) {
                    AE::log error => "failed db operations: $@";
                    undef $tm;
                    $w->send();
                }
                else {
                    $tm = $create_timer->();
                }
              }
        }
    )->();

    die unless $w->recv;
    AE::log info => "db say goodbye!";

}

sub update_db_hook {
    my ( $self, $name, $cnt, $cb ) = @_;
    return unless $cnt;
    my $t0 = AE::time;
    $cb->();
    my $elapsed = AE::time - $t0;
    AE::log(
        ( $elapsed > 0.5 ? "note" : "info" ) =>
          sprintf "Adding %i %s records took %.4f sec",
        $cnt, $name, $elapsed
    );
}

sub update_db {
    my $self   = shift;
    my $p      = $self->{parser};
    my $db     = $self->{db};
    my $hcache = $self->{hosts_cache};
    my $ucache = $self->{users_cache};

    my ( $stat, $hosts, $users ) =
      @{ $self->{stats} }
      ? $p->flatten( shift @{ $self->{stats} }, $hcache, $ucache )
      : ( [], [], [] );

    $self->update_db_hook(
        "host",
        scalar @$hosts,
        sub {
            $db->add_to_hosts($hosts);
        }
    );

    $self->update_db_hook(
        "user",
        scalar @$users,
        sub {
            $db->add_to_users($users);
        }
    );

    $self->update_db_hook(
        "stat",
        @$stat / 7,
        sub {
            my ( $i, $u ) = $db->add_to_stat($stat);
            AE::log note => "updated stat with $i inserts and $u updates";
        }
    );

    $self->update_db_hook(
        "access",
        @{ $self->{records} } / 15,
        sub {
            $db->add_to_access( $self->{records} );
            $self->{records} = [];
        }
    );
}

1
