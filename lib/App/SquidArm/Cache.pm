package App::SquidArm::Cache;
use strict;
use warnings;
use AnyEvent;
use AnyEvent::Handle;
use AnyEvent::Socket;
use JSON;

sub new {
    my $class = shift;
    bless {@_}, $class;
}

sub listen {
    my $self = shift;
    my $c    = $self->{cache};

    tcp_server $self->{host}, $self->{port}, sub {
        my ( $fh, $peer_host, $peer_port ) = @_;
        my $h;
        my $data = '';
        $h = AnyEvent::Handle->new(
            fh       => $fh,
            on_error => sub {
                AE::log error => $_[2];
                $_[0]->destroy;
            },
            on_eof => sub {
                AE::log warn => "EOF";
                $h->destroy;
            },
            on_read => sub {
                $data .= $h->{rbuf};
                $h->{rbuf} = '';
                do {
                    last if $data !~ /\x0d?\x0a\x0d?\x0a/g;
                    my $end = pos($data);
                    pos($data) = 0;
                    if ( $data !~ m#^GET (/[^ ]+) HTTP/1\.1\x0d?\x0a#g ) {
                        AE::log warn => "got broken HTTP request";
                        $h->push_shutdown;
                    }
                    else {
                        my ( $path, $query ) = split /\?/, $1, 2;
                        $path =~ s/%([0-9A-Fa-f]{2})/chr(hex($1))/eg;
                        my $response = eval { $self->router($path) };
                        if ($@) {
                            $h->push_write( "500 Internal Server Error"
                                  . "\x0d\x0d\x0d\x0a" );
                        }
                        else {
                            $h->push_write($response);
                        }
                    }
                    substr( $data, 0, $end, '' );
                } while ( length $data );
            }
        );
      }
}

sub router {
    my ( $self, $path ) = @_;
    my $psgi = [ 200, [], [] ];
    my ( $ts, $user ) = ( split "/", $path )[ 1, 2 ];
    if ( $ts !~ /^\d{4}\-\d{2}\-\d{2} \d{2}\:\d{2}$/ ) {
        $psgi->[0] = 404;
    }
    else {
        if ( defined $user && length $user ) {
            $psgi->[2] = $self->get_user_traf( $ts, $user );
        }
        else {
            $psgi->[2] = $self->get_traf($ts);
        }
    }
    return _psgi2http($psgi);
}

sub _psgi2http {
    my $psgi = shift;

    my $json = encode_json( $psgi->[2] // [] );

    join "\x0d\x0a",
      $psgi->[0] . " Ok", @{ $psgi->[1] },
      'Content-Type: application/json',
      'Content-Length: ' . length($json), '',
      $json;
}

sub get_traf {
    my ( $self, $ts ) = @_;
    my $c = $self->{cache};

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
    my $c = $self->{cache};

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

1;
