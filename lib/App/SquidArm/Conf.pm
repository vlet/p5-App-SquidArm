package App::SquidArm::Conf;
use strict;
use warnings;
use Carp;

sub new {
    my ( $class, %opts ) = @_;
    bless {%opts}, $class;
}

sub parse {
    my $self = shift;

    croak 'no configuration file' if !exists $self->{conf};
    croak "can't open configuration file: $!"
      if !open my $fh, '<', $self->{conf};

    $self->{raw_conf} = do { local $/; <$fh> };
    close $fh;
    $self->_parser;
    $self;
}

sub tag {
    my ( $self, $tag, $value ) = @_;
    if ( @_ >= 3 ) {
        push @{ $self->{tags}->{$tag} }, ref $value ? @$value : $value;
        $self;
    }
    elsif ( @_ == 2 ) {
        $self->{tags}->{$tag};
    }
}

sub _parser {
    my $self = shift;
    my $line = 0;
    my @errors;
    for ( split /\015?\012\015?/, $self->{raw_conf} ) {
        $line++;
        next if /^\s*(?:\#|$)/;

        # remove inline comments
        s/\s\#\s.+$//g;
        $self->tag( split ' ', $_, 2 );
    }
}

1;
