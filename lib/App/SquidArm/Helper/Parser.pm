package App::SquidArm::Helper::Parser;
use strict;
use warnings;
use parent qw(App::SquidArm::Helper);
use AnyEvent;
use AnyEvent::Handle;
use App::SquidArm::Cache;

sub end {
    my $self = shift;
    $self->{parser_pipe}->destroy
      if $self->{parser_pipe};
    $self->{db_pipe}->destroy
      if $self->{db_pipe};
    undef $self->{m};
}

sub begin {

    my ( $self, $rd, $db_wr ) = @_;

    $self->unload_modules('App::SquidArm::Log')
      if $self->conf('parser_reload_on_restart');
    require App::SquidArm::Log;

    my $p =
      App::SquidArm::Log->new( ignore_denied => $self->conf('ignore_denied') );

    my $readed = 0;
    my $w      = AE::cv;

    my $cleanup = sub {
        my $status = shift;
        $self->store_memcache(1);
        $self->{db_pipe}->on_drain(
            sub {
                $w->send($status);
            }
        );
    };

    my $hup = AE::signal HUP => sub {
        AE::log crit => "got HUP";
        $w->send();
    };

    my $term = AE::signal TERM => sub {
        AE::log crit => "got TERM";
        $cleanup->(1);
    };

    my $int = AE::signal INT => sub {
        AE::log crit => "got INT";
        $cleanup->(1);
    };

    $self->{memcache} ||= {};
    my $m_host = $self->conf('mcache_host') || '127.0.0.1';
    my $m_port = $self->conf('mcache_port') || '8001';

    my ( $h, $db );

    $self->{m} = App::SquidArm::Cache->new(
        host  => $m_host,
        port  => $m_port,
        cache => $self->{memcache},
    )->listen;

    $db = $self->{db_pipe} = AnyEvent::Handle->new(
        fh       => $db_wr,
        on_error => sub {
            AE::log error => "db pipe error $!";
            $w->send();
        },
    );

    $h = $self->{parser_pipe} = AnyEvent::Handle->new(
        fh      => $rd,
        on_read => sub {
            return unless length( $h->{rbuf} );
            AE::log debug => "parser got " . length( $h->{rbuf} );
            my $len;
            my $records = ['records'];
            for ( 1 .. 2 ) {
                $len = $p->parser( \$h->{rbuf}, $records, $self->{memcache} );
                last if defined $len;

                # first line failed in parser
                # find first \n position
                my $i = index( $h->{rbuf}, "\012" );
                if ( $i == -1 ) {
                    return;
                }
                my $junk = substr( $h->{rbuf}, 0, $i + 1, '' );
                AE::log error => "parser find junk at start position:\n"
                  . $junk;
            }
            if ( !defined $len ) {
                AE::log error => 'parser: malformed input';
                return;
            }
            substr( $h->{rbuf}, 0, $len, '' );
            $readed += $len;
            AE::log debug => "parser read $len (total $readed)";
            AE::log debug => "send records to db pipe";
            $db->push_write( storable => $records );
            $self->store_memcache();
        },
        on_error => sub {
            AE::log error => "reader error $!";
            AE::log debug => "readed $readed";
            $cleanup->();
        },
        on_eof => sub {
            AE::log debug => "readed $readed";
            $cleanup->(1);
        }
    );

    die unless $w->recv;
    AE::log info => "parser say goodbye!";
}

sub store_memcache {
    my ( $self, $force ) = @_;

    my @ts = (localtime)[ 5, 4, 3, 2 ];
    $ts[0] += 1900;
    $ts[1]++;
    my $current = sprintf "%d-%02d-%02d %02d:00", @ts;
    my @list = keys %{ $self->{memcache} };

    for my $ts (@list) {
        next if !$force && $ts ge $current;
        AE::log debug => "send stat for '$ts' to db pipe";
        $self->{db_pipe}->push_write(
            storable => [ "stats", { $ts => delete $self->{memcache}->{$ts} } ]
        );
    }
}

1
