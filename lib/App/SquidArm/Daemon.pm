package App::SquidArm::Daemon;
use strict;
use warnings;
use Storable qw(freeze);
use AnyEvent;
use AnyEvent::Fork;
use AnyEvent::Handle;
use AnyEvent::Socket;
use AnyEvent::Log;

sub new {
    my ( $class, %opts ) = @_;
    my $on_error = delete $opts{on_error};
    bless {
        conf => {%opts},
        ( $on_error && ref $on_error eq 'CODE' )
        ? ( on_error => $on_error )
        : (),
    }, $class;
}

sub conf {
    $_[0]->{conf}->{ $_[1] };
}

sub hup { }

sub stop {
    shift->{stop} = 1;
}

sub listen {
    my ( $self, %opts ) = @_;
    my $port    = $self->conf('port') || 8000;
    my $host    = $self->conf('host');
    my $allowed = $self->conf('allowed');
    my $on_data = delete $opts{on_data};
    my $on_eof  = delete $opts{on_eof};

    my @pipes;
    for my $i ( 0 .. 1 ) {
        push @pipes, AnyEvent::Handle->new(
            fh       => $self->{pipe_w}->[$i],
            on_error => sub {
                $_[0]->destroy;
                AE::log error => "writer error $!";
            }
        ) if exists $self->{pipe_w}->[$i];
    }
    AE::log info => "starting server on $host:$port";

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

        $handle = AnyEvent::Handle->new(
            fh       => $fh,
            on_error => sub {
                AE::log error => $_[2];
                $_[0]->destroy;
            },
            on_eof => sub {
                $handle->destroy;
                AE::log info => "$peer_host:$peer_port disconnected";
            },
            on_read => sub {
                my $len = rindex( $handle->{rbuf}, "\012" ) + 1;
                return unless $len;
                AE::log trace => "parser will read $len of "
                  . length( $handle->{rbuf} );
                my $rbuf = substr $handle->{rbuf}, 0, $len, '';
                for (@pipes) {
                    $_->push_write($rbuf);
                }
                $on_data->( \$rbuf ) if defined $on_data;
            }
        );
        ();
    };

    $self;
}

sub run {
    my ( $self, %opts ) = @_;

    # Pipes:
    # 0  - Daemon    -> LogParser
    # 1  - Daemon    -> LogWriter
    # 2  - LogParser -> MemCache
    # 3  - LogParser -> DBStore
    # 4  - MemCache  -> DBStore

    for my $i ( 0 .. 4 ) {
        pipe $self->{pipe_r}->[$i], $self->{pipe_w}->[$i]
          or die "pipe failed\n";
    }

    $self->listen(%opts);

    $self->exec( 'LogParser',
        fh =>
          [ $self->{pipe_w}->[2], $self->{pipe_w}->[3], $self->{pipe_r}->[0] ],
    );
    $self->exec( 'LogWriter', fh => [ $self->{pipe_r}->[1] ], );
    $self->exec( 'MemCache',
        fh => [ $self->{pipe_w}->[4], $self->{pipe_r}->[2] ], );

    $self->exec( 'DBStore',
        fh => [ $self->{pipe_r}->[3], $self->{pipe_r}->[4] ], );
}

sub exec {
    my ( $self, $name, %opts ) = @_;
    my $restart_timeout = 60;
    my $time            = AnyEvent->time;
    my $proc            = AnyEvent::Fork->new_exec;
    my $pid             = $proc->pid;

    $proc->require("App::SquidArm::$name");
    $proc->send_fh( @{ $opts{fh} } ) if exists $opts{fh};
    $proc->send_arg( freeze( $self->{conf} ),
        exists $opts{args} ? @{ $opts{args} } : () );
    $proc->run(
        "App::SquidArm::${name}::run",
        sub {
            $self->{proc_h}->{$name} = AnyEvent::Handle->new(
                fh      => shift,
                on_read => sub { },
                on_eof  => sub {
                    $self->{proc_h}->{$name}->destroy;
                    return if $self->{stop};
                    AE::log error => "$name disconnected";

                    my $dt = AnyEvent->time - $time;
                    waitpid $pid, 0;
                    if ( $dt < 1 ) {
                        AE::log error => "$name dying too fast: $dt sec."
                          . " Will try to restart $name in $restart_timeout sec";
                        $self->{restart_timer}->{$name} =
                          AE::timer $restart_timeout, 0, sub {
                            $self->exec( $name, %opts );
                          };
                    }
                    else {
                        AE::log info => "$name restarted";
                        $self->exec( $name, %opts );
                    }
                },
                on_error => sub {
                    $_[0]->destroy;
                    AE::log error => "Error on master handle of $name: $_[2]";
                    if ($pid) {
                        kill 'TERM', $pid;
                        waitpid $pid, 0;
                    }
                    AE::log error =>
                      "Will try to restart $name in $restart_timeout sec";
                    $self->{restart_timer}->{$name} =
                      AE::timer $restart_timeout, 0, sub {
                        $self->exec( $name, %opts );
                      };
                }
            );
        }
    );
}

1
