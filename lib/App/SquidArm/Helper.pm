package App::SquidArm::Helper;
use strict;
use warnings;
use AnyEvent;
use AnyEvent::Log;
use AnyEvent::Debug;
use Storable qw(thaw);
use Carp;

sub new {
    my ( $class, %opts ) = @_;
    my @miss = grep { !exists $opts{$_} } (qw(conf master_fh));
    croak "Missing options in new: @miss" if @miss;

    bless {
        conf      => thaw( delete $opts{conf} ),
        master_fh => delete $opts{master_fh},
        %opts
    }, $class;
}

sub conf {
    $_[0]->{conf}->{ $_[1] };
}

sub init_logging {
    my $self = shift;

    my $log_level = $self->conf('log_level') || 'warn';

    AnyEvent::Log::ctx->title( ref $self );
    $AnyEvent::Log::FILTER->level($log_level);
    $AnyEvent::Log::LOG->log_to_file( $self->conf('log_file') )
      if defined $self->conf('log_file');

    AE::log info => 'init logging';

    $self;
}

sub handle_master_pipe {
    my ( $self, $error_cb ) = @_;
    $self->{master_h} = AnyEvent::Handle->new(
        fh       => $self->{master_fh},
        on_read  => sub { },
        on_error => sub {
            $_[0]->destroy;
            AE::log info => "lost connection to master: $_[2]";
            $self->cleanup($error_cb);
        }
    );
    $self;
}

sub handle_signals {
    my ( $self, $cb ) = @_;
    for my $signal (qw(HUP TERM)) {
        $self->{signal}->{$signal} = AE::signal $signal => sub {
            AE::log info => "got signal $signal, cleanup";
            $self->cleanup($cb);
          }
    }
    $self->{signal}->{INT} = AE::signal INT => sub {
        AE::log info => 'ignore INT';
    };
    $self;
}

sub run {
    croak 'must be overriden';
}

sub cleanup {
    my ( $self, $cb ) = @_;
    $cb->();
}

sub init_debugging {
    my $self   = shift;
    my $socket = $self->conf('debug_unixsocket');
    return $self unless $socket;

    $socket .= '_' . lc( ( split( '::', ref $self ) )[2] );
    $self->{SHELL} = AnyEvent::Debug::shell "unix/", $socket;
    AE::log info => "created unix socket $socket for debugging";
    $self;
}

1
