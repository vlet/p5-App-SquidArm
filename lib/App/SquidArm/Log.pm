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
    my ( $self, $buf_ref, $records, $stats, $max_rec ) = @_;
    my @data;

    while ( $$buf_ref =~ /$squid_log_re/gc ) {

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

        push @$records, @data;
        next unless defined $data[8];

        my @ts = ( localtime( $data[0] ) )[ 5, 4, 3, 2 ];
        $ts[0] += 1900;
        $ts[1]++;
        my $ts = sprintf "%d-%02d-%02d %02d:00", @ts;
        my $key = defined $data[10] ? $data[10] : $data[3];
        if ( index( $data[4], 'DENIED' ) == -1 ) {
            if ( index( $data[4], 'HIT' ) != -1 ) {
                $stats->{$ts}->{$key}->{ $data[8] }->{hit} += $data[6];
            }
            else {
                $stats->{$ts}->{$key}->{ $data[8] }->{miss} += $data[6];
            }
        }
        $stats->{$ts}->{$key}->{ $data[8] }->{req}++;
    }

    my $i = pos $$buf_ref;
    return $i;
}

sub flatten {
    my ( $self, $stats, $hcache, $ucache ) = @_;
    my ( @stat, @hosts, @users );
    for my $ts ( keys %$stats ) {
        for my $user ( keys %{ $stats->{$ts} } ) {
            if ( !exists $ucache->{$user} ) {
                push @users, $user;
                $ucache->{$user} = 1;
            }
            for my $host ( keys %{ $stats->{$ts}->{$user} } ) {
                push @stat, $ts, $user, $host,
                  map { $stats->{$ts}->{$user}->{$host}->{$_} || 0 }
                  (qw(miss hit req));
                if ( !exists $hcache->{$host} ) {
                    push @hosts, $host;
                    $hcache->{$host} = 1;
                }
            }
        }
    }
    return \@stat, \@hosts, \@users;
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
