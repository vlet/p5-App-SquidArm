package App::SquidArm::LogWriter;
use strict;
use warnings;
use AnyEvent;
use AnyEvent::Handle;
use parent qw(App::SquidArm::Helper);

sub run {
    my ( $fh, $pipe, $conf ) = @_;
    my $self = App::SquidArm::LogWriter->new(
        conf      => $conf,
        master_fh => $fh,
        pipe      => $pipe,
    );

    my $w = AE::cv;
    #<<< dear perltidy, please don't ruin this nice formatting
    $self
        ->init_logging
        ->init_debugging
        ->handle_master_pipe($w)
        ->handle_signals($w)
        ->handle_access_log
        ->rotate_log
        ->handle_log_pipe
        ;
    #>>>
    $w->recv;
}

sub handle_access_log {
    my $self = shift;

    open my $access_fh, '>>', $self->{conf}->{access_log} or die $!;
    AE::log info => "openning access log";
    $self->{access_log_h} = AnyEvent::Handle->new(
        fh       => $access_fh,
        on_error => sub {
            AE::log error => $_[2];
            $_[0]->destroy;
        }
    );

    $self;
}

sub rotate_log {
    my $self = shift;
    $self->{timer} = AE::timer 0, 60, sub {
        my $size = -s $self->{access_log_h}->{fh};
        if ( $size >= 100_000_000 ) {
            AE::log info => "access log has size $size b., rotate";
            $self->{access_log_h}->destroy;
            rename $self->{conf}->{access_log},
              $self->{conf}->{access_log} . "_" . time;
            $self->handle_access_log;
        }
    };
    $self;
}

sub handle_log_pipe {
    my $self = shift;

    my $handle;
    $handle = AnyEvent::Handle->new(
        fh       => $self->{pipe},
        on_error => sub {
            AE::log error => $_[2];
            $_[0]->destroy;
        },
        on_eof => sub {
            $handle->destroy;
            AE::log info => "Done";
        },
        on_read => sub {
            AE::log debug => "receive " . length( $handle->{rbuf} ) . " bytes";
            if ( defined $self->{access_log_h} ) {
                $self->{access_log_h}->push_write( $handle->{rbuf} );
            }
            $handle->{rbuf} = undef;
        }
    );

    $self;
}

1
