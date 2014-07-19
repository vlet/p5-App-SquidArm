use strict;
use warnings;
use Test::More;
use Test::TCP;
use AnyEvent;
use AnyEvent::Handle;
use AnyEvent::Socket;
use JSON;

BEGIN {
    use_ok 'App::SquidArm::Cache';
}

subtest 'new' => sub {
    new_ok 'App::SquidArm::Cache';
};

subtest 'cache server' => sub {
    test_tcp(
        client => sub {
            my $port  = shift;
            my $w     = AE::cv;
            my @tests = (
                [
                    "/2014-07-16%2006:00" =>
                      [ "2014-07-16 06:00", 1000, 100, 10, 0 ],
                ],
                [
                    "/2014-07-16%2006:00/" =>
                      [ "2014-07-16 06:00", 1000, 100, 10, 0 ],
                ],
                [ "/2014-07-16%2005:00" => [] ],
                [
                    "/2014-07-16%2006:00/vasya" =>
                      [ "2014-07-16 06:00", "vasya", 1000, 100, 10, 0 ]
                ]
            );

            tcp_connect '127.0.0.1', $port, sub {
                my $h;
                my $len;
                $h = AnyEvent::Handle->new(
                    fh     => shift,
                    on_eof => sub {
                        $h->destroy;
                        $w->send;
                    },
                    on_error => sub {
                        fail;
                        $h->destroy;
                        $w->send;
                    },
                    on_read => sub {
                        for my $data ( split /\x0d\x0a\x0d\x0a/, $h->{rbuf} ) {
                            if ($len) {
                                my $d = substr $data, 0, $len;
                                my $test = shift @tests;
                                is_deeply decode_json($d), $test->[1];
                                if ( !@tests ) {
                                    $h->destroy;
                                    $w->send;
                                    last;
                                }
                            }
                            if ( $data =~ /Content-Length: (\d+)/ ) {
                                $len = $1;
                            }
                            else {
                                $len = undef;
                            }
                        }
                        $h->{rbuf} = '';
                    }
                );
                for (@tests) {
                    $h->push_write(
                        join "\x0d\x0a",
                        "GET $_->[0] HTTP/1.1",
                        "Host: 127.0.0.1",
                        "", ""
                    );
                }
            };
            $w->recv;
        },
        server => sub {
            my $port  = shift;
            my $w     = AE::cv;
            my $cache = App::SquidArm::Cache->new(
                host  => '127.0.0.1',
                port  => $port,
                cache => {
                    '2014-07-16 06:00' => {
                        'vasya' => {
                            'example.com' => {
                                hit    => 100,
                                miss   => 1000,
                                req    => 10,
                                denied => 0
                            }
                        }
                    }
                }
            )->listen;
            $w->recv;
        }
    );
};

done_testing
