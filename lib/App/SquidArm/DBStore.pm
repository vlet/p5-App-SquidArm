package App::SquidArm::DBStore;
use strict;
use warnings;
use AnyEvent;
use AnyEvent::Handle;
use App::SquidArm::DB;
use parent qw(App::SquidArm::Helper);

sub run {
    my ( $fh, $pipe_parser, $pipe_memcache, $conf ) = @_;
    my $self = App::SquidArm::DBStore->new(
        conf          => $conf,
        master_fh     => $fh,
        pipe_parser   => $pipe_parser,
        pipe_memcache => $pipe_memcache,
        records       => [],
        stats         => [],
    );

    my $w = AE::cv;
    $self->init_logging->init_debugging->handle_signals($w)
      ->handle_master_pipe($w);

    my $db = $self->{db} = App::SquidArm::DB->new(
        db_driver => $self->conf('db_driver'),
        db_dir    => $self->conf('db_dir'),
        tz        => $self->conf('tz'),
    );
    $db->create_tables;

    # Init caches
    $self->{hosts_cache} = { map { $_ => 1 } @{ $db->get_hosts } };
    $self->{users_cache} = { map { $_ => 1 } @{ $db->get_users } };

    #<<< dear perltidy, please don't ruin this nice formatting
    $self
        ->handle_parser_pipe
        ->handle_memcache_pipe
        ->update_timer($w)
        ;
    #>>>

    $w->recv;
}

sub update_timer {
    my ( $self, $cb ) = @_;
    my $interval = $self->conf('db_update_interval') || 1;

    $self->{timer} = AE::timer $interval, 0, sub {
        eval { $self->update_db(); };
        if ($@) {
            AE::log error => "failed db operations: $@";
            $cb->() if $cb;
        }
        else {
            $self->update_timer($cb);
        }
    };
    $self;
}

sub handle_parser_pipe {
    my $self = shift;
    $self->{pp_h} = AnyEvent::Handle->new(
        fh       => $self->{pipe_parser},
        on_error => sub {
            AE::log error => $_[2];
            $_[0]->destroy;
        },
        on_read => sub {
            $_[0]->push_read(
                storable => sub {
                    if ( defined $_[1] && ref $_[1] eq "ARRAY" ) {
                        push @{ $self->{records} }, @{ $_[1] };
                    }
                    else {
                        AE::log error => "got malformed records";
                    }
                }
            );
        }
    );
    $self;
}

sub handle_memcache_pipe {
    my $self = shift;
    $self->{mc_h} = AnyEvent::Handle->new(
        fh       => $self->{pipe_memcache},
        on_error => sub {
            AE::log error => $_[2];
            $_[0]->destroy;
        },
        on_read => sub {
            $_[0]->push_read(
                storable => sub {
                    if ( defined $_[1] && ref $_[1] eq "HASH" ) {
                        push @{ $self->{stats} }, $_[1];
                    }
                    else {
                        AE::log error => "got malformed records";
                    }
                }
            );
        }
    );
    $self;
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
    my $db     = $self->{db};
    my $hcache = $self->{hosts_cache};
    my $ucache = $self->{users_cache};

    $self->update_db_hook(
        "access",
        @{ $self->{records} } / 15,
        sub {
            $db->add_to_access( $self->{records} );
            $self->{records} = [];
        }
    );

    return unless @{ $self->{stats} };

    my ( $stat, $gstat, $hosts, $users ) =
      $self->flatten( shift @{ $self->{stats} }, $hcache, $ucache );

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
            AE::log info => "updated stat with $i inserts and $u updates";
        }
    );

    $self->update_db_hook(
        "gstat",
        @$gstat / 6,
        sub {
            my ( $i, $u ) = $db->add_to_gstat($gstat);
            AE::log note => "updated gstat with $i inserts and $u updates";
        }
    );

}

sub flatten {
    my ( $self, $stats, $hcache, $ucache ) = @_;
    my ( @stat, @gstat, %gstat, @hosts, @users );
    for my $ts ( keys %$stats ) {
        my $gts = substr $ts, 0, 10;
        for my $user ( keys %{ $stats->{$ts} } ) {
            if ( !exists $ucache->{$user} ) {
                push @users, $user;
                $ucache->{$user} = 1;
            }
            for my $host ( keys %{ $stats->{$ts}->{$user} } ) {
                push @stat, $ts, $user, $host,
                  map { $stats->{$ts}->{$user}->{$host}->{$_} || 0 }
                  (qw(miss hit req denied));
                for (qw(miss hit req denied)) {
                    $gstat{$gts}{$user}{$_} +=
                      $stats->{$ts}->{$user}->{$host}->{$_} || 0;
                }
                if ( !exists $hcache->{$host} ) {
                    push @hosts, $host;
                    $hcache->{$host} = 1;
                }
            }
        }
    }
    for my $gts ( keys %gstat ) {
        for my $user ( keys %{ $gstat{$gts} } ) {
            push @gstat, $gts, $user,
              map { $gstat{$gts}->{$user}->{$_} || 0 }
              (qw(miss hit req denied));
        }
    }
    return \@stat, \@gstat, \@hosts, \@users;
}

1
