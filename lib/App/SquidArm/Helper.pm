package App::SquidArm::Helper;
use strict;
use warnings;
use AnyEvent;
use Carp;

sub new {
    my $class = shift;
    bless {@_}, $class;
}

sub begin {
    croak 'must be overriden';
}

sub end {
    croak 'must be overriden';
}

sub conf {
    shift->{conf}->{ shift() };
}

sub eval_loop {
    my $self = shift;
    my $proc = ref $self;

    while (1) {
        my $start = AnyEvent->time;
        eval { $self->begin(@_) };
        my $duration = AnyEvent->time - $start;

        if ($@) {
            $self->end();
            AE::log error => "$proc die with error (after $duration sec): $@";
            if ( $duration < 1 ) {
                AE::log error => "don't restart $proc, " . "it dying too fast";
            }
            else {
                AE::log error => "restart $proc";
                next;
            }
        }
        else {
            $self->end();
            AE::log info => "$proc normal exit: elapsed $duration sec";
        }
        last;
    }
    exit;
}

sub unload_modules {
    my $self = shift;
    for my $m (@_) {
        my $path = join( '/', split /::/, $m ) . '.pm';
        next if !exists $INC{$path};
        delete $INC{$path};
        {
            no strict 'refs';
            for my $sym ( keys %{ $m . '::' } ) {
                delete ${ $m . '::' }{$sym};
            }
        }
    }
}

1
