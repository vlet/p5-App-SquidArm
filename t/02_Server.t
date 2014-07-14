use strict;
use warnings;
use Test::More;
use Test::TCP;
use AnyEvent;
use AnyEvent::Socket;
use IO::Handle;
use File::Temp qw(tempfile);
use AnyEvent::AIO;
use IO::AIO;
use Errno qw(EAGAIN EINTR);
use Text::Diff;

BEGIN {
    use_ok 'App::SquidArm::Server';
}

my $test_data = <<'EOF';
1402454773.542    307 192.168.1.1 TCP_MISS/200 10333 GET http://kevingreeneitblog.blogspot.com/feeds/posts/default user@DOMAIN.LOCAL HIER_DIRECT/74.125.205.132 application/atom+xml
1402454774.269    539 192.168.1.2 TCP_MISS/301 655 GET http://blogs.msdn.com/rds/rss.xml user2@DOMAIN.LOCAL HIER_DIRECT/65.52.103.94 -
1402454775.886   1614 192.168.1.3 TCP_MISS/200 200966 GET http://blogs.msdn.com/b/rds/rss.aspx user3@DOMAIN.LOCAL HIER_DIRECT/65.52.103.94 text/xml
EOF

subtest 'server' => sub {

    my ( $rd, $wr );
    pipe $rd, $wr or BAIL_OUT "pipe failed";
    my ( undef, $db_fname ) = tempfile( UNLINK => 1 );

    test_tcp(
        client => sub {
            my $port = shift;
            my $w    = AE::cv;
            my $w2;
            close $wr;
            tcp_connect '127.0.0.1', $port, sub {
                my $fh = shift;
                print $fh $test_data;
                $w->send();
            };

            $w->recv;
            my $len = $rd->getline;
            chomp $len;
            is $len, length($test_data), "got bytes";
        },
        server => sub {
            my $port = shift;
            my $w    = AE::cv;

            close $rd;
            $wr->autoflush(1);

            App::SquidArm::Server->new(
                host        => '127.0.0.1',
                port        => $port,
                db_file     => $db_fname,
                db_driver   => 'sqlite',
                mcache_port => $port + 1,
              )->listen(
                on_data => sub {
                    my $data_ref = shift;
                    print $wr length($$data_ref) . "\n";
                }
              );
            $w->recv;
        }
    );
};

subtest 'writing' => sub {

    my ( $fh,   $fname )    = tempfile( UNLINK => 1 );
    my ( undef, $db_fname ) = tempfile( UNLINK => 1 );
    my $alog = 't/access.log';
    close $fh;

    my ( $rd, $wr );
    pipe $rd, $wr or die "pipe failed";

    test_tcp(
        client => sub {
            my $port = shift;
            close $wr;
            my $w = AE::cv;
            my $w2;
            my $size;
            tcp_connect '127.0.0.1', $port, sub {
                my $ffh = shift;
                open my $log_fh, '<', $alog or die $!;
                $size = -s $log_fh;
                my $fu;
                my $off = 0;
                my $l   = $size;
                $fu = sub {
                    aio_sendfile $ffh, $log_fh, $off, $l, sub {
                        if ( $_[0] == -1 ) {
                            if ( $!{EAGAIN} || $!{EINTR} ) {
                                $fu->();
                            }
                            else {
                                AE::log error => "$!";
                            }
                            return;
                        }
                        AE::log info => "send $_[0] bytes of access.log "
                          . "$off - "
                          . ( $off + $_[0] )
                          . " of $size";
                        if ( $_[0] < $l ) {
                            $off += $_[0];
                            $l -= $_[0];
                            $fu->();
                        }
                        else {
                            close $ffh;
                            close $log_fh;
                        }
                    };
                    ();
                };
                $fu->();
                ();
            };

            my $handle;
            my $t;
            $handle = AnyEvent::Handle->new(
                fh      => $rd,
                on_read => sub {
                    $handle->push_read(
                        line => sub {
                            my ( $hdl, $len ) = @_;
                            AE::log info => "get line $len";
                            $w->send($len);
                            return;
                        }
                    );
                },
                on_error => sub {
                    AE::log error => "error";
                    $w->send(0);
                }
            );

            my $len = $w->recv;
            is $len, $size, "send/receive";
        },
        server => sub {
            my $port = shift;
            my $w    = AE::cv;
            my $t;

            close $rd;
            my $handle = AnyEvent::Handle->new( fh => $wr );

            App::SquidArm::Server->new(
                host        => '127.0.0.1',
                port        => $port,
                access_log  => $fname,
                db_file     => $db_fname,
                db_driver   => 'sqlite',
                mcache_port => $port + 1,
              )->listen(
                on_eof => sub {
                    my $rsize = shift;
                    $handle->push_write("$rsize\n");
                    AE::log info => "called EOF man\n";
                    $handle->on_drain(
                        sub {
                            AE::log info => "drain\n";
                            $w->send();
                        }
                    );
                    return;
                },
              );
            $w->recv;
            AE::log info => "exit server\n";
            exit;
        }
    );
    my $diff = diff $alog, $fname;
    ok( ( $diff eq '' ), "no diff " ) or do {
        my $a = -s $alog;
        my $f = -s $fname;
        note "$a - $f  = " . ( $a - $f );
    };
    sleep 1;
};

done_testing;
