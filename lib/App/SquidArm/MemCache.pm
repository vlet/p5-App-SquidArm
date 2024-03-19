package App::SquidArm::MemCache;
use strict;
use warnings;
use AnyEvent;
use AnyEvent::Handle;
use AnyEvent::Socket;
use JSON;
use DateTime;
use parent qw(App::SquidArm::Helper);

sub run {
    my ( $fh, $memcache_pipe, $parser_pipe, $conf ) = @_;

    my $self = App::SquidArm::MemCache->new(
        conf          => $conf,
        master_fh     => $fh,
        parser_pipe   => $parser_pipe,
        memcache_pipe => $memcache_pipe,
        stats         => {},
    );

    $self->cht(time);

    my $w = AE::cv;

    #<<< dear perltidy, please don't ruin this nice formatting
    $self
      ->init_logging
      ->init_debugging
      ->handle_memcache_pipe
      ->handle_memcache_server
      ->handle_parser_pipe
      ->handle_signals($w)
      ->handle_master_pipe($w)
      ;
    #>>>
    $w->recv;
}

sub cleanup {
    my ( $self, $cb ) = @_;
    $self->push_memcache('FORCE');
    $self->{pp_h}->push_shutdown;
    $self->{mc_h}->on_drain(
        sub {
            $self->{mc_h}->push_shutdown;
            $cb->();
        }
    );
}

sub handle_memcache_pipe {
    my $self = shift;
    $self->{mc_h} = AnyEvent::Handle->new(
        fh       => $self->{memcache_pipe},
        on_error => sub {
            AE::log error => $_[2];
            $_[0]->destroy;
        }
    );
    $self;
}

sub handle_parser_pipe {
    my $self = shift;
    $self->{pp_h} = AnyEvent::Handle->new(
        fh       => $self->{parser_pipe},
        on_error => sub {
            AE::log error => $_[2];
            $_[0]->destroy;
        },
        on_read => sub {
            $self->{pp_h}->push_read(
                storable => sub {
                    if ( defined $_[1] && ref $_[1] eq "ARRAY" ) {
                        $self->update_stat( $_[1] );
                        $self->push_memcache;
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

# Timestamp from start of hour of epoch
sub hour_ts {
    my ( $self, $epoch ) = @_;
    DateTime->from_epoch( epoch => $epoch, time_zone => $self->conf('tz') )
      ->truncate( to => 'hour' )->epoch;
}

sub cht {
    my ( $self, $epoch ) = @_;
    my $r = \$self->{current_hour_timestamp};
    defined $epoch ? $$r = $self->hour_ts($epoch) : $$r;
}

sub update_stat {
    my ( $self, $r ) = @_;
    my $cht    = $self->cht;
    my $stats  = $self->{stats};
    my $ignore = $self->conf('ignore_denied');

    while ( my @data = splice @$r, 0, 15 ) {
        next unless defined $data[8];
        $cht = $self->cht( $data[0] )
          if $cht > $data[0] || $cht + 3600 <= $data[0];

        # Key is username // ip
        my $key = defined $data[10] ? $data[10] : $data[3];
        if ( index( $data[4], 'DENIED' ) == -1 ) {
            if ( index( $data[4], 'HIT' ) != -1 ) {
                $stats->{$cht}->{$key}->{ $data[8] }->{hit} += $data[6];
            }
            else {
                $stats->{$cht}->{$key}->{ $data[8] }->{miss} += $data[6];
            }
            $stats->{$cht}->{$key}->{ $data[8] }->{req}++;
        }
        elsif ( !$ignore ) {
            $stats->{$cht}->{$key}->{ $data[8] }->{denied}++;
        }
    }
}

sub push_memcache {
    my ( $self, $force ) = @_;
    my $stats = $self->{stats};
    my $cht   = $self->cht;
    for my $ts ( keys %$stats ) {
        next unless $force || $ts < $cht;
        AE::log info => "send stat for '$ts' to db pipe";
        $self->{mc_h}
          ->push_write( storable => { $ts => delete $self->{stats}->{$ts} } );
    }
}

sub handle_memcache_server {
    my $self = shift;
    my $c    = $self->{stats};

    my $port    = $self->conf('memcache_port') || 8080;
    my $host    = $self->conf('memcache_host');
    my $allowed = $self->conf('memcache_allowed');

    $self->{server} = tcp_server $host, $port, sub {
        my ( $fh, $peer_host, $peer_port ) = @_;
        my $h;
        $h = AnyEvent::Handle->new(
            fh       => $fh,
            on_error => sub {
                AE::log error => $_[2];
                $_[0]->destroy;
            },
            on_eof => sub {
                AE::log info => "client $peer_host:$peer_port disconnected";
                $h->destroy;
            },
            on_read => sub {
                $self->handle_http_request( @_, $peer_host );
                ();
            }
        );
    };
    $self;
}

sub handle_http_request {
    my ( $self, $h, $peer_host ) = @_;
    do {
        last if $h->{rbuf} !~ /\x0d?\x0a\x0d?\x0a/;
        if ( $h->{rbuf} !~ m#^GET (/[^ ]+) HTTP/1\.1\x0d?\x0a# ) {
            AE::log warn => "got broken HTTP request";
        }
        else {
            my ( $raw_path, $query ) = split /\?/, $1, 2;
            ( my $path = $raw_path ) =~ s/%([0-9A-Fa-f]{2})/chr(hex($1))/eg;
            my $psgi = eval { $self->router($path) };
            my $response;
            if ($@) {
                $psgi = [ 500, [], ['Internal Server Error'] ];
                AE::log warn => "Request '$path' triggered internal error: $@";
            }
            $response = _psgi2http($psgi);
            AE::log info => qq|HTTP: $peer_host GET "$raw_path" $psgi->[0] |
              . length($response);
            AE::log debug => "Response:\n$response";
            $h->push_write($response);
        }
        $h->{rbuf} = '';
        $h->push_shutdown;
    } while ( length $h->{rbuf} );
}

sub router {
    my ( $self, $path ) = @_;
    my $psgi = [
        200,
        [
            'Content-Type' => 'application/json',
        ],
        ['[]']
    ];
    my ( $date, $user ) = ( split "/", $path )[ 1, 2 ];
    if ( $date !~ /^(\d{4})\-(\d{2})\-(\d{2})T(\d{2})\:\d{2}$/ ) {
        $psgi->[0] = 404;
    }
    else {
        my $ts = DateTime->new(
            year      => $1,
            month     => $2,
            day       => $3,
            hour      => $4,
            time_zone => $self->conf('tz'),
        )->epoch;

        my $res =
          defined $user && length $user
          ? $self->get_user_traf( $ts, $user )
          : $self->get_traf($ts);
        $psgi->[2] = [ encode_json($res) ] if @$res;
    }
    $psgi;
}

my %codes = (
    200 => 'Ok',
    404 => 'Not Found',
    500 => 'Internal Server Error',
);

sub _psgi2http {
    my $psgi = shift;

    my @headers = ();
    for my $i ( 0 .. @{ $psgi->[1] } / 2 - 1 ) {
        push @headers,
          $psgi->[1]->[ 2 * $i ] . ': ' . $psgi->[1]->[ 2 * $i + 1 ];
    }
    my $body = join '', @{ $psgi->[2] };

    join "\x0d\x0a", "HTTP/1.1 $psgi->[0] $codes{ $psgi->[0] }",
      @headers,
      'Content-Length: ' . length($body), '',
      $body;
}

sub get_traf {
    my ( $self, $ts ) = @_;
    my $c = $self->{stats};

    return [] if !exists $c->{$ts};
    my %res = ();
    for my $user ( keys %{ $c->{$ts} } ) {
        for my $host ( keys %{ $c->{$ts}->{$user} } ) {
            $res{$_} += $c->{$ts}->{$user}->{$host}->{$_}
              for (
                qw(miss hit req
                denied)
              );
        }
    }
    return [ $ts, map { $res{$_} || 0 } (qw(miss hit req denied)) ];
}

sub get_user_traf {
    my ( $self, $ts, $user ) = @_;
    my $c = $self->{stats};

    return [] unless exists $c->{$ts} && exists $c->{$ts}->{$user};
    my %res = ();
    for my $host ( keys %{ $c->{$ts}->{$user} } ) {
        $res{$_} += $c->{$ts}->{$user}->{$host}->{$_}
          for (
            qw(miss hit req
            denied)
          );
    }
    return [ $ts, $user, map { $res{$_} || 0 } (qw(miss hit req denied)) ];
}

1
