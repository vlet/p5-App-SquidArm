package App::SquidArm::Conf;
use strict;
use warnings;
use Carp;
use MIME::Base64;

my @tags = (
    qw( host port access_log db_driver db_dir db_update_interval ignore_denied
      log_level log_file allowed cachemgr mhost mport tz memcache_port
      memcache_host debug_unixsocket dist_dir)
);

sub multitag {
    my ( $self, $tag, $value ) = @_;
    push @{ $self->{tags}->{$tag} }, $value;
    ();
}

sub boolean {
    my ( $self, $tag, $value ) = @_;
    if ( !$value || lc($value) eq "false" ) {
        $value = 0;
    }
    else {
        $value = 1;
    }
    $self->{tags}->{$tag} = $value;
    ();
}

my %filter = (
    allowed  => \&multitag,
    cachemgr => sub {
        my ( $self, $tag, $value ) = @_;
        my ( $host, $user, $pass ) = split ' ', $value, 3;
        ( $host, my $port ) = split /:/, $host;
        $self->multitag( $tag,
            [ $host, $port || 3128, encode_base64( $user . ':' . $pass, '' ) ]
        );
    },
    ignore_denied => \&boolean,
);

sub new {
    my ( $class, %opts ) = @_;
    bless {%opts}, $class;
}

sub parse {
    my $self = shift;

    croak 'no configuration file' if !exists $self->{config};
    croak "can't open configuration file: $!"
      if !open my $fh, '<', $self->{config};

    $self->{raw_conf} = do { local $/; <$fh> };
    close $fh;
    $self->_parser;
    $self;
}

sub tag {
    my ( $self, $tag, $value ) = @_;
    unless ( grep { $tag eq $_ } @tags ) {
        carp "unknown tag $tag";
        return undef;
    }

    if ( @_ == 3 ) {
        if ( exists $filter{$tag} ) {
            $filter{$tag}->( $self, $tag, $value );
        }
        else {
            $self->{tags}->{$tag} = $value;
        }
        $self;
    }
    elsif ( @_ == 2 ) {
        $self->{tags}->{$tag};
    }
}

sub tags {
    shift->{tags};
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
