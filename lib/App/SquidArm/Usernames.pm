package App::SquidArm::Usernames;
use strict;
use warnings;
use Carp;
use Encode qw(decode_utf8);

my %sources = (
    default => sub {
        $_[0]
    },
    system => sub {
        decode_utf8( ( getpwnam($_[0]) )[6] ) || $_[0]
    },
);

sub new {
    my ( $class, %opts ) = @_;
    $opts{source} = 'default' if !exists $opts{source};
    croak "no handler exists for source $opts{source}" if !exists $sources{ $opts{source} };

    bless {
        %opts,
        cache => {},
    }, $class;
}

sub source {
    $_[0]->{source};
}

sub pairs {
    my $self = shift;
    my @ret = ();
    for my $user (@_) {
        push @ret, $user,
            exists $self->{cache}->{$user} ?
                $self->{cache}->{$user} :
                ( $self->{cache}->{$user} =
                    $user =~ /^[\d\.]$/ ? $user : $sources{ $self->source }->($user) );
    }
    return @ret
}

1
