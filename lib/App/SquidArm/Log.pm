package App::SquidArm::Log;
use strict;
use warnings;

#use re 'debug';

# squid log format %ts.%03tu %6tr %>a %Ss/%03>Hs %<st %rm %ru %[un %Sh/%<a %mt
my $squid_log_re = qr/\G
    (\d+)\.(\d{3})  # 1,2 unixtime + msec
    \s+
    (\d+)           # 3 Response time (msec)
    \s
    ([\d\.]+)       # 4 Client ip
    \s
    ([A-Z_]+)       # 5 Squid status  
    \/
    (\d{3})         # 6 Client status
    \s
    (\d+)           # 7 Sent reply size
    \s
    (\w+)           # 8 Request method
    \s
    (\S+)           # 9 URL
    \s
    (?:
        (?:\-?(\S+?)?)  # 10 username
        (?:\@([\S]+))?  # 11 realm
    )
    \s
    (\w+)   # 12 Hierarchy status
    \/
    (?:\-?([\d\.\-\:a-f]+)?) # 13 server ip
    \s
    (?:\-?(\S+)?)           # 14 mime type
    \n
/x;

my $url_re = qr/^
    (?:[a-zA-Z]+\:\/\/)? # scheme
    ([a-zA-Z\d\-\.]+)    # 1 Host
    (?:\:\d+)?           # port
    (\/\S*)?             # 2 URI
$/x;

sub new {
    bless {}, shift;
}

sub parser {
    my ( $self, $buf_ref, $cb_access, $cb_stat, $max_rec ) = @_;
    my $store = [];
    my $stat  = [];
    my $st    = {};
    my $rec   = 0;
    $max_rec ||= 1e10;
    my @data;

    while ( $$buf_ref =~ /$squid_log_re/gc ) {
        $rec++;

        @data = (
            $1,  $2, $3,               $4, $5, $6, $7, $8,
            $9,  undef, # URL -> HOST, URI
            $10, $11,
            $12, $13, $14
        );

        if ( $data[8] =~ $url_re ) {
            $data[8] = $1;
            $data[9] = $2;
        }
        else {
            $data[8] = undef;
        }

        push @$store, @data;

        if ( $cb_access && $rec >= $max_rec ) {
            $cb_access->($store);
            $store = [];
            $rec   = 0;
        }

        my @ts = ( localtime( $data[0] ) )[ 5, 4, 3, 2 ];
        $ts[0] += 1900;
        $ts[1]++;
        my $ts = sprintf "%d-%02d-%02d %02d:00", @ts;
        my $key = defined $data[10] ? $data[10] : $data[3];
        if ( index( $data[4], 'DENIED' ) == -1 ) {
            if ( index( $data[4], 'HIT' ) != -1 ) {
                $st->{$ts}->{$key}->{hit} += $data[6];
            }
            else {
                $st->{$ts}->{$key}->{miss} += $data[6];
            }
        }
        $st->{$ts}->{$key}->{req}++;
    }

    $cb_access->($store) if $cb_access && $rec;

    for my $ts ( keys %$st ) {
        for my $user ( keys %{ $st->{$ts} } ) {
            push @$stat, $ts, $user,
              map { $st->{$ts}->{$user}->{$_} || 0 } (qw(miss hit req));
        }
    }

    $cb_stat->($stat) if $cb_stat && @$stat;

    my $i = pos $$buf_ref;
    return $i;
}

=cut

sub parser1 {
    my ($self, $buf_ref) = @_;
    my $i = 0;
    my $user = $self->{user};
    my $ip   = $self->{ip};
    for ( split /\n/, $$buf_ref ) {
        $i += length($_) + 1;
        my @data = split ' ';
        next if index($data[3], 'DENIED') != -1;
        my $key = $data[7] eq '-' ? $data[2] : $data[7];
        if ( index($data[3], 'HIT') != -1 ) {
            $user->{$key}->{hit}  += $data[4];
        } else {
            $user->{$key}->{miss} += $data[4];
        }
        if ($data[7] ne '-' &&
            ( !exists $ip->{$key} || !exists $ip->{$key}->{$data[2]}))
        {
                my $str = join " ", (keys %{ $ip->{$key} }), $data[2], $data[0];
                $ip->{$key} = { $data[2] => $str };
        }
    }
    return $i
}

=cut

1;
