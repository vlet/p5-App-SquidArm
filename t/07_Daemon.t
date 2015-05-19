use strict;
use warnings;
use Test::More;
use Test::TCP;
use AnyEvent;
use AnyEvent::Socket;
use Storable qw(freeze);
use File::Temp qw(tempfile);

use lib 't/lib';

BEGIN {
    use_ok 'App::SquidArm::Daemon';
}

subtest 'new' => sub {
    new_ok 'App::SquidArm::Daemon';
};

subtest 'listen' => sub {

    my $test_data = "xyz\n";
    pipe my $rd, my $wr or die "pipe failed";

    test_tcp(
        client => sub {
            my $port = shift;
            my $w    = AE::cv;
            close $wr;
            tcp_connect '127.0.0.1', $port, sub {
                my $fh = shift or die $!;
                print $fh $test_data;
                $w->send;
            };
            $w->recv;
            is $rd->getline, $test_data;
        },
        server => sub {
            my $port = shift;
            my $w    = AE::cv;
            close $rd;
            $wr->autoflush(1);

            my $daemon = App::SquidArm::Daemon->new(
                host => '127.0.0.1',
                port => $port,
              )->listen(
                on_data => sub {
                    my $data_ref = shift;
                    print $wr $$data_ref;
                }
              );
            $w->recv;
        }
    );
};

subtest 'exec' => sub {

    pipe my $rd, my $wr or die "pipe failed";

    test_tcp(
        client => sub {
            my $port = shift;
            my $w    = AE::cv;
            my ( $t, $h );
            $h = AnyEvent::Handle->new(
                fh      => $rd,
                on_read => sub {
                    is $h->{rbuf}, "hi\n", "received hi";
                    $t = AnyEvent->timer( after => 0.2, cb => $w );
                },
                on_error => sub { }
            );
            $w->recv;
        },
        server => sub {
            my $port   = shift;
            my $w      = AE::cv;
            my $daemon = App::SquidArm::Daemon->new(
                host => '127.0.0.1',
                port => $port,
            )->listen;

            $daemon->exec( 'Test', fh => [$wr] );
            $w->recv;
        }
    );
};

subtest 'exec LogWriter' => sub {
    plan skip_all => 'fix later';
    my ( $fh, $temp ) = tempfile( UNLINK => 1 );
    close $fh;
    my $test = "test data\n";
    pipe my $rd, my $wr or die "pipe failed";
    my $daemon = App::SquidArm::Daemon->new( access_log => $temp );
    $daemon->exec( 'LogWriter', fh => [$rd] );
    undef $rd;
    my $w = AE::cv;
    my $h = AnyEvent::Handle->new(
        fh       => $wr,
        on_error => sub { },
    );
    $h->push_write($test);
    my $t = AnyEvent->timer( after => 0.2, cb => $w );
    $w->recv;
    open $fh, '<', $temp or die $!;
    my $data = do { local $/; <$fh> };
    close $fh;
    is $data, $test;
};

done_testing;
