package App::SquidArm::Server;
use strict;
use warnings;
use AnyEvent;
use AnyEvent::Handle;
use AnyEvent::Socket;
use AnyEvent::AIO;
use IO::AIO;
use App::SquidArm::Helper::Parser;
use App::SquidArm::Helper::DB;

sub new {
    my ( $class, %opts ) = @_;
    my $on_error = delete $opts{on_error};
    my $self     = bless {
        queue       => '',
        aio         => 0,
        written     => 0,
        conf        => {%opts},
        records     => [],
        stats       => {},
        hosts_cache => {},
        users_cache => {},
        ( $on_error && ref $on_error eq 'CODE' )
        ? ( on_error => $on_error )
        : (),
    }, $class;

    $self->init_parser;

    $self;
}

# Fork separated parser/db processes
sub init_parser {
    my $self = shift;
    my ( $rd, $wr );
    pipe $rd, $wr or die "pipe failed\n";

    my $pid = fork();
    die "fork failed\n" unless defined $pid;

    # Parent
    if ($pid) {
        $0 = "squid arm [main]";
        $self->{parser_child} = AnyEvent->child(
            pid => $pid,
            cb  => sub {
                my ( $pid, $status ) = @_;
                AE::log critical => "parser helper PID $pid is dead: "
                  . ( $status >> 8 );
                if ( exists $self->{on_error} ) {
                    $self->{on_error}->cb();
                }
                else {
                    die;
                }
            }
        );
        $self->{child_pid} = $pid;
        close $rd;
        $self->{parser_pipe} = AnyEvent::Handle->new(
            fh       => $wr,
            on_error => sub {
                AE::log error => "writer error $!";
            }
        );
    }

    # Child
    else {
        close $wr;

        my ( $db_rd, $db_wr );
        pipe $db_rd, $db_wr or die "db pipe failed\n";

        my $pid = fork();
        die "fork failed\n" unless defined $pid;

        # Parser process
        if ($pid) {
            $0 = "squid arm [parser]";
            $self->{db_child} = AnyEvent->child(
                pid => $pid,
                cb  => sub {
                    my ( $pid, $status ) = @_;
                    AE::log critical => "db helper PID $pid is dead: "
                      . ( $status >> 8 );
                    die;
                }
            );
            $self->{child_pid} = $pid;
            close $db_rd;
            my $parser =
              App::SquidArm::Helper::Parser->new( conf => $self->{conf} );
            $parser->eval_loop( $rd, $db_wr );
        }

        # DB process
        else {
            $0 = "squid arm [db]";
            close $db_wr;
            my $db = App::SquidArm::Helper::DB->new( conf => $self->{conf} );
            $db->eval_loop($db_rd);
        }
    }
}

sub openlog {
    my $self = shift;
    open $self->{access_log_fh}, '>>', $self->conf('access_log') or die $!;
}

sub writelog {
    my ( $self, $data ) = @_;
    AE::log debug => ++$self->{num} . " " . length( $self->{queue} );
    $self->{queue} .= $data if length($data);
    return if $self->{aio} || length( $self->{queue} ) == 0;

    my $copy = $self->{queue};
    $self->{queue} = '';
    AE::log debug => "begin writing of "
      . ( length($copy) )
      . " bytes by aio_write at "
      . $self->{written};

    $self->{aio} = 1;
    aio_write $self->{access_log_fh}, undef, length($copy), $copy, 0, sub {
        my $wrtn = shift;
        $self->{aio} = 0;
        AE::log debug => "remove kostyl";
        undef $self->{kostyl};

        if ( $wrtn < 0 ) {
            AE::log critical => "bad wr status";
            return;
        }

        #AE::log critical => sysseek( $self->{access_log_fh},0,1 ). " tell";
        $self->{written} += $wrtn;
        if ( $wrtn < length($copy) ) {
            AE::log error => "written less than expected: $wrtn < "
              . length($copy);
            $self->{queue} =
              substr( $copy, $wrtn, length($copy) - $wrtn ) . $self->{queue};
        }
        AE::log debug => "written $wrtn bytes, total $self->{written}";
        $self->writelog() if length( $self->{queue} );
        1;
    };
    AE::log debug => "register kostyl";
    $self->{kostyl} = AE::timer 0.5, 0, sub {
        AE::log note => "kostyl started";
        undef $self->{kostyl};
        my $t0 = AnyEvent->time;

        #IO::AIO::flush;
        IO::AIO::poll_wait;
        my $t1 = AnyEvent->time;
        AE::log debug => sprintf "poll_wait elapsed %0.5fs\n", ( $t1 - $t0 );
        IO::AIO::poll_cb;
        AE::log
          debug => sprintf "poll_cb   elapsed %0.5fs\n",
          ( AnyEvent->time - $t1 );
    };

    1;
}

sub stop {
    my $self = shift;
    undef $self->{server};
    $self->{access_log_fh}->sync if $self->{access_log_fh};
    $self;
}

sub hup {
    my ( $self, $conf ) = @_;
    AE::log error => "error on closing access_log: $!"
      unless close $self->{access_log_fh};
    $self->{conf} = $conf;
    undef $self->{server};
    $self->listen();
    kill HUP => $self->{child_pid};
}

sub conf {
    shift->{conf}->{ shift() };
}

sub listen {
    my ( $self, %opts ) = @_;
    my $port    = $self->conf('port') || 8000;
    my $host    = $self->conf('host');
    my $allowed = $self->conf('allowed');
    my $on_data = delete $opts{on_data};
    my $on_eof  = delete $opts{on_eof};
    $self->openlog if defined $self->conf('access_log');

    $self->{server} = tcp_server $host, $port, sub {
        my ( $fh, $peer_host, $peer_port ) = @_;
        if ( defined $allowed
            && !grep { $peer_host eq $_ } @$allowed )
        {
            AE::log note => "$peer_host is not allowed";
            undef $fh;
            return;
        }

        AE::log info => "connected $peer_host:$peer_port";

        my $handle;
        my $readed = 0;

        $handle = AnyEvent::Handle->new(
            fh       => $fh,
            on_error => sub {
                AE::log error => $_[2];
                $_[0]->destroy;
            },
            on_eof => sub {
                $handle->destroy;
                AE::log info => "Done.";
                if ( $readed && $on_eof ) {
                    $self->{parser_pipe}->on_drain(
                        sub {
                            IO::AIO::flush if $self->{aio} == 1;
                            $on_eof->($readed);
                        }
                    );
                }
                return;
            },
            on_read => sub {
                my $len = rindex( $handle->{rbuf}, "\012" ) + 1;
                return unless $len;
                AE::log debug => "parser will read $len from "
                  . length( $handle->{rbuf} );
                my $rbuf = substr $handle->{rbuf}, 0, $len, '';
                $self->{parser_pipe}->push_write($rbuf);
                $readed += $len;
                $on_data->( \$rbuf ) if defined $on_data;
                if ( defined $self->{access_log_fh} ) {
                    $self->writelog($rbuf);
                }
                1;
            }
        );
        ();
    };

    $self;
}

1
