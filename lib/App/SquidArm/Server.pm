package App::SquidArm::Server;
use strict;
use warnings;
use AnyEvent;
use AnyEvent::Handle;
use AnyEvent::Socket;
use AnyEvent::AIO;
use IO::AIO;

sub new {
    my ( $class, %opts ) = @_;
    my $self = bless {
        queue   => '',
        aio     => 0,
        written => 0,
        conf    => {%opts},
    }, $class;

    $self->init_parser;

    $self;
}

# Fork separated parser process
sub init_parser {
    my $self = shift;
    my ( $rd, $wr );
    pipe $rd, $wr or die "pipe failed\n";

    my $pid = fork();
    die "fork failed\n" unless defined $pid;

    # Parent
    if ($pid) {
        $self->{parser_child} = AnyEvent->child(
            pid => $pid,
            cb  => sub {
                my ( $pid, $status ) = @_;
                AE::log critical => "parser helper PID $pid is dead: "
                  . ( $status >> 8 );
                die;
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
        my $start;
        my $xyz = 0;
        while (1) {
            $start = AnyEvent->time;
            eval { $self->parser_helper($rd); };
            my $duration = AnyEvent->time - $start;

            if ($@) {
                $self->{parser_pipe}->destroy if $self->{parser_pipe};
                $self->{db}->disconnect       if $self->{db};
                AE::log error =>
                  "parser die with error (after $duration sec): $@";
                if ( $duration < 1 ) {
                    AE::log error => "don't restart parser, "
                      . "it dying too fast";
                }
                else {
                    AE::log error => "restart parser";
                    next;
                }
            }
            else {
                AE::log info => "parser normal exit: elapsed $duration sec";
            }
            last;
        }
        exit;
    }
}

sub parser_helper {

    my ( $self, $rd ) = @_;

    require App::SquidArm::Log;
    require App::SquidArm::DB;

    my $p = App::SquidArm::Log->new;
    use Data::Dumper;
    print Dumper ($self);
    print Dumper $self->conf('db_driver');
    my $db = $self->{db} = App::SquidArm::DB->new(
        db_driver => $self->conf('db_driver'),
        db_file   => $self->conf('db_file'),
    );
    $db->create_tables;

    AE::log debug => "parser init db";

    my $readed = 0;
    my $w      = AE::cv;

    AE::signal HUP => sub {
        $w->send();
    };

    AE::signal TERM => sub {
        $w->send(1);
    };

    my $h;
    $h = $self->{parser_pipe} = AnyEvent::Handle->new(
        fh      => $rd,
        on_read => sub {
            return unless length( $h->{rbuf} );
            AE::log debug => "parser got " . length( $h->{rbuf} );

            #my $ilen = rindex($h->{rbuf},"\012") + 1;
            #AE::log debug => 'parser find \n at position ' . $ilen;
            #return unless $ilen;
            #my $copy = substr( $h->{rbuf}, 0, $ilen,'');
            my $len;
            for ( 1 .. 2 ) {
                eval {
                    $db->begin;
                    $len = $p->parser(
                        \$h->{rbuf},
                        sub {
                            AE::log info => "adding "
                              . ( @{ $_[0] } / 15 )
                              . " access records";
                            $db->add_to_access( $_[0] );
                        },
                        sub {
                            AE::log info => "adding "
                              . ( @{ $_[0] } / 5 )
                              . " stat records";
                            $db->add_to_stat( $_[0] );
                        },
                        $App::SquidArm::DB::MAX_INSERT,
                    );
                    $db->end;
                };
                if ($@) {
                    $self->{save} = $h->{rbuf};
                    $w->send();
                    AE::log error => "parser error: $@";
                    return;
                }
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
        },
        on_error => sub {
            AE::log error => "reader error $!";
            AE::log debug => "readed $readed";
            $w->send();
        },
        on_eof => sub {
            AE::log debug => "readed $readed";
            $w->send(1);
        }
    );
    $h->{rbuf} = $self->{save} if $self->{save};

    die unless $w->recv;
    AE::log info => "parser say goodbye!";
}

sub openlog {
    my $self = shift;
    open $self->{access_log_fh}, '>', $self->conf('access_log') or die $!;
}

#=cut

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
        AE::log warn => "kostyl started";
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

=cut

sub writelog {
    my ($self, $data) = @_;
    push @{ $self->{queue} }, sub {
        aio_write $self->{access_log_fh}, undef, length($data), $data, 0, sub {
            my $wrtn = shift;
            $self->{aio} = 0;
            #undef $self->{kostyl};

            AE::log error => "bad wr status" if $wrtn < 0;
            #AE::log debug => sysseek( $self->{access_log_fh}, 0, 1 ). " tell";
            $self->{written} += $wrtn;
            AE::log error => "written less than expected" if $wrtn < length($data);
            AE::log debug=> "write $wrtn bytes: from buffer of total $self->{written}";
            if (@{ $self->{queue} }) {
                $self->writelog()
            }
            1;
        };
        #$self->{kostyl} = AE::timer 0.1, 0, sub {
        #    my $t0 = AnyEvent->time;
        #    IO::AIO::flush;
        #    AE::log warn => sprintf "flush elapsed %0.5fs\n", (AnyEvent->time - $t0);
        #    undef $self->{kostyl};
        #};
        1
    } if length $data;

    return if $self->{aio} || !@{ $self->{queue} };
    AE::log info => "start aio";
    $self->{aio} = 1;
    my $aio_cb = shift @{ $self->{queue} };
    $aio_cb->();
    return
}

#=cut

sub writelog_c {
    my ($self, $data) = @_;
    return if length($data) == 0;
    push @{ $self->{queue} }, $data;

    AE::log debug => "begin writing of " . (length($data)). " bytes by aio_write at $self->{written}";

    AE::log critical => ++$self->{num};
    AE::log critical => IO::AIO::nready() . " + " . IO::AIO::npending();
 
    return if $self->{grp};
    
    my $grp = $self->{grp} = aio_group sub {
        AE::log critical => "all done";
        undef $self->{grp};
    };

    limit $grp 1;

    feed $grp sub {
        AE::log critical => "need to feeding?";
        my $data = shift @{ $self->{queue} } or return;
        AE::log critical => "feeding";

        aio_write $self->{access_log_fh}, undef, length($data), $data, 0, sub {
            my $r = shift;
            AE::log critical => "! $!" if $r < 0;
            $self->{written} += $r;
            AE::log critical => "! less written" if $r < length($data);

            AE::log debug => "write $r bytes: from buffer of total $self->{written}";
            1;
        };
        1;
    };
   
    1
}

=cut

sub stop {
    my $self = shift;
    undef $self->{server};
    $self;
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
                AE::log info => "parser will read $len from "
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
