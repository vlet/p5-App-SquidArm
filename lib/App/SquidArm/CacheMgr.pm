package App::SquidArm::Cache;
use strict;
use warnings;
use AnyEvent;
use AnyEvent::Handle;
use AnyEvent::Socket;

my %pages = (
    'info' => {
        uptime   => qr/UP Time:\s+([\d\.]+)/,
        cputime  => qr/CPU Time:\s+([\d\.]+)/,
        clients  => qr/accessing cache:\s+(\d+)/,
        memtotal => qr/via sbrk\(\):\s+(\d+)/,
        memsize  => qr/Mem size:\s+(\d+)/,
        swapsize => qr/Swap size:\s+(\d+)/,
        numfd    => qr/file desc currently in use:\s+(\d+)/,
    },
    'counters' => {
        clireqs   => qr/client_http.requests = (\d+)/,
        clihits   => qr/client_http.hits = (\d+)/,
        clierrs   => qr/client_http.errors = (\d+)/,
        cliin     => qr/client_http.kbytes_in = (\d+)/,
        cliout    => qr/client_http.kbytes_out = (\d+)/,
        clihitout => qr/client_http.hit_kbytes_out = (\d+)/,
    }
);

=cut

server.all.requests = 17303296
server.all.errors = 0
server.all.kbytes_in = 380818114
server.all.kbytes_out = 24006751
unlink.requests = 5409650
page_faults = 14512
select_loops = 556922459
cpu_time = 68652.902425
wall_time = 10.226338
swap.outs = 5389186
swap.ins = 2804818
swap.files_cleaned = 19
aborted_requests = 425509

=cut

sub new {
    my $class = shift;
    bless {@_}, $class;
}

sub run {
    my ( $self, $conf ) = @_;

    my $w = AE::cv;
    my %res;

    for my $squid ( @{ $conf->tag('cachemgr') } ) {
        for my $page ( keys %pages ) {
            $w->begin();
            http_request(
                @$squid, $page,
                sub {
                    my ( $headers, $data ) = @_;
                    for my $tag ( keys %{ $pages{$page} } ) {
                        ( $res{$tag} ) = ( $data =~ $pages{$page}->{$tag} );
                    }
                    $w->end;
                },
                sub {
                    AE::log error =>
"request to $squid->[0]:$squid->[1] for page $page failed\n";
                    $w->end;
                }
            );
        }

        $w->begin();
        http_request(
            @$squid,
            'active_requests',
            sub {
                my ( $headers, $data ) = @_;
                for my $conn ( split /\x0a\x0a/, $data ) {
                    AE::log info => $conn;
                    last;
                }
                $w->end;
            },
            sub {
                AE::log error =>
"request to $squid->[0]:$squid->[1] for page active_requests failed\n";
                $w->end;
            }
        );
    }

    $w->recv;

}

sub http_request {
    my ( $host, $port, $auth, $page, $cb_ok, $cb_err ) = @_;
    tcp_connect $host, $port, sub {
        my $fh = shift or do {
            $cb_err->($!);
            return;
        };

        my $h;
        my $data = '';

        $h = AnyEvent::Handle->new(
            fh       => $fh,
            on_error => sub {
                $h->destroy;
                $cb_err->();
            },
            on_eof => sub {
                AE::log info => "got answer length " . ( length $data );
                $cb_ok->( split /\x0d\x0a\x0d\x0a/, $data, 2 );
                $h->destroy;
            },
            on_read => sub {
                $data .= $h->{rbuf};
                $h->{rbuf} = '';
            }
        );
        $h->push_write(
            join "\x0d\x0a",
            "GET /squid-internal-mgr/$page HTTP/1.0",
            "Authorization: Basic $auth",
            "", ""
        );
    };
    ();
}
