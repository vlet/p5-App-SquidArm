use strict;
use warnings;
use Test::More;
use Errno qw(EAGAIN EINTR EWOULDBLOCK);
use Time::HiRes qw(nanosleep);

# This test shows that openned pipe can be reused many times
# Child can close pipe, this doesn't affect parent
subtest 'pipe_close' => sub {
    pipe my $r, my $w or die $!;
    my $test = "test\n";

    note "child write to pipe";
    for ( 1 .. 2 ) {
        my $pid = fork;
        die $! unless defined $pid;

        if ( !$pid ) {
            close $r;
            my $len = syswrite $w, $test;
            die "can't write to pipe\n" unless defined $len;
            close $w;
            exit;
        }

        sysread $r, my $data, 5;
        is $data, $test, "parent received correct data";
        waitpid $pid, 0;
    }

    note "child read from pipe";
    for ( 1 .. 2 ) {
        my $pid = fork;
        die $! unless defined $pid;

        if ( !$pid ) {
            close $w;
            sysread $r, my $data, 5;
            exit( $data eq $test );
        }
        my $len = syswrite $w, $test;
        die "can't write to pipe\n" unless defined $len;
        waitpid $pid, 0;
        ok( ( $? >> 8 ), "child ok" );
    }
};

# This test shows that pipe buffer is shared between processes
# Possible race condition: read from the same pipe by several processes
subtest 'shared pipe for reading' => sub {
    pipe my $r, my $w or die $!;
    my $test = "12";

    my $pid1 = fork;
    die $! unless defined $pid1;
    if ( !$pid1 ) {
        close $w;
        sysread $r, my $data, 1;
        note "child1 got $data";
        exit $data;
    }

    my $pid2 = fork;
    die $! unless defined $pid2;
    if ( !$pid2 ) {
        close $w;
        sysread $r, my $data, 1;
        note "child2 got $data";
        exit $data;
    }

    my $len = syswrite $w, $test;
    die "can't write to pipe\n" unless defined $len;
    waitpid $pid1, 0;
    my $child1_code = ( $? >> 8 );
    waitpid $pid2, 0;
    my $child2_code = ( $? >> 8 );
    isnt $child1_code, $child2_code, "codes differs";
};

# Several processes can write to the same pipe simultaneously
# syswrite messages are atomic when blocking if messages are small
# (less than buffer)
subtest 'shared pipe for writing (blocking)' => sub {
    pipe my $r, my $w or die $!;
    my $count  = 500;
    my $p1     = 263;
    my $p2     = 257;
    my $string = ' ' x ( $count * ( $p1 + $p2 ) );
    $string = '';

    $r->blocking(1);
    $w->blocking(1);

    my $child = sub {
        my $data = shift;
        close $r;
        for ( 1 .. $count ) {
            my $l = syswrite $w, $data;
            nanosleep( int( rand(50_000) ) );
        }
        exit;
    };

    my $pid1 = fork;
    die $! unless defined $pid1;
    if ( !$pid1 ) {
        $child->( "1" x $p1 );
    }

    my $pid2 = fork;
    die $! unless defined $pid2;
    if ( !$pid2 ) {
        $child->( "2" x $p2 );
    }

    my $len = 0;
    while ( $len < $count * ( $p1 + $p2 ) ) {
        my $p = int( rand(500) ) + 1;
        my $l = sysread( $r, my $data, $p );
        die "can't write from buffer" unless defined $l;
        $len += $l;
        $string .= $data;
    }
    waitpid $pid1, 0;
    waitpid $pid2, 0;
    $string =~ s/(1{$p1})/+/g;
    $string =~ s/(2{$p2})/-/g;
    is scalar( $string =~ y/+// ), $count, "$count full segments of child 1";
    is scalar( $string =~ y/-// ), $count, "$count full segments of child 2";
    $string =~ s/(.{64})/$1\n/g;
    note $string;
};

# Several processes can write to the same pipe simultaneously
# syswrite messages are atomic also when non-blocking
subtest 'shared pipe for writing (non-blocking)' => sub {
    pipe my $r, my $w or die $!;
    my $count  = 500;
    my $p1     = 263;
    my $p2     = 257;
    my $string = ' ' x ( $count * ( $p1 + $p2 ) );
    $string = '';

    $r->blocking(0);
    $w->blocking(0);

    my $child = sub {

        # Close reader or we will never receive sigpipe
        close $r;

        # sig pipe will interupt our code, so ignore it,
        # we will handle it later
        $SIG{PIPE} = 'IGNORE';

        my $buf = my $data = shift;
        for ( 1 .. $count ) {
            my $l = syswrite $w, $buf;
            if ( !defined $l ) {
                die $! if $! != EAGAIN && $! != EINTR && $! != EWOULDBLOCK;
                nanosleep( int( rand(50_000) ) );
                redo;
            }
            elsif ( $l == length($buf) ) {
                $buf = $data;
                nanosleep( int( rand(50_000) ) );
            }
            else {
                note "partial write $l <> $p1";
                substr $buf, 0, $l, '';
                redo;
            }
        }
        exit;
    };

    my $pid1 = fork;
    die $! unless defined $pid1;
    if ( !$pid1 ) {
        $child->( "1" x $p1 );
    }
    my $pid2 = fork;
    die $! unless defined $pid2;
    if ( !$pid2 ) {
        $child->( "2" x $p2 );
    }

    my $len = 0;
    while ( $len < $count * ( $p1 + $p2 ) ) {
        my $p = int( rand(500) ) + 1;
        my $l = sysread( $r, my $data, $p );
        if ( !defined $l && $! != EAGAIN && $! != EINTR && $! != EWOULDBLOCK ) {
            die $!;
        }
        elsif ( !defined $l ) {
            nanosleep( int( rand(50_000) ) );
        }
        else {
            $len += $l;
            $string .= $data;
        }
    }
    waitpid $pid1, 0;
    waitpid $pid2, 0;
    $string =~ s/(1{$p1})/+/g;
    $string =~ s/(2{$p2})/-/g;
    is scalar( $string =~ y/+// ), $count, "$count full segments of child 1";
    is scalar( $string =~ y/-// ), $count, "$count full segments of child 2";
    $string =~ s/(.{64})/$1\n/g;
    note $string;
};

# Several processes can write to the same pipe simultaneously
# syswrite messages are atomic also when non-blocking
subtest 'broken shared pipe for writing (non-blocking)' => sub {
    plan skip_all => 'may not work';
    use Fcntl;

    pipe my $r, my $w or die $!;

    note "buffer:  " . fcntl( $w, 1032, 0 ) . "\n";
    fcntl( $r, 1031, 512 * 1024 );
    note "buffer:  " . fcntl( $r, 1032, 0 ) . "\n";

    my $count  = 100;
    my $p1     = 135000;
    my $p2     = 133000;
    my $string = ' ' x ( $count * ( $p1 + $p2 ) );
    $string = '';

    $r->blocking(0);
    $w->blocking(0);

    my $child = sub {

        # Close reader or we will never receive sigpipe
        close $r;

        # sig pipe will interupt our code, so ignore it,
        # we will handle it later
        $SIG{PIPE} = 'IGNORE';

        my $buf = my $data = shift;
        my $sum = 0;
        for ( 1 .. $count ) {
            my $l = syswrite $w, $buf;
            $sum += $l if defined $l;
            if ( !defined $l ) {
                die "$$: sum if $sum. Error: $!"
                  if $! != EAGAIN && $! != EINTR && $! != EWOULDBLOCK;
                nanosleep( int( rand(50_000) ) );
                redo;
            }
            elsif ( $l == length($buf) ) {
                $buf = $data;
                nanosleep( int( rand(50_000) ) );
            }
            else {
                note "partial write $l <> $p1";
                substr $buf, 0, $l, '';
                redo;
            }
        }
        if ( $sum != length($data) * $count ) {
            note "$$: $sum != " . ( length($data) * $count );
        }
        else {
            note "write full $sum";
        }
        exit;
    };

    my $pid1 = fork;
    die $! unless defined $pid1;
    if ( !$pid1 ) {
        $child->( "1" x $p1 );
    }
    my $pid2 = fork;
    die $! unless defined $pid2;
    if ( !$pid2 ) {
        $child->( "2" x $p2 );
    }

    my $len = 0;
    while ( $len < $count * ( $p1 + $p2 ) ) {
        my $p = $p1 + $p2 + 1;
        my $l = sysread( $r, my $data, $p );
        if (   !defined $l
            && $! != EAGAIN
            && $! != EINTR
            && $! != EWOULDBLOCK )
        {
            die $!;
        }
        elsif ( !defined $l ) {

            #nanosleep( int(rand(50_000)) );
        }
        else {
            $len += $l;
            $string .= $data;
        }
    }
    close $r;
    note "waitpids";
    waitpid $pid1, 0;
    waitpid $pid2, 0;

    my $xp = sub {
        my ( $l, $digit, $size, $replace ) = @_;
        my $o = $l % $size;
        my $d = int( $l / $size );
        ( $replace x $d ) . ( $o ? "$digit:$o " : "" );
    };

    $string =~ s/(1+)/ $xp->(length($1),"1",$p1,"+")/eg;
    $string =~ s/(2+)/ $xp->(length($1),"2",$p2,"-")/eg;
    is scalar( $string =~ y/+// ), $count, "$count full segments of child 1";
    is scalar( $string =~ y/-// ), $count, "$count full segments of child 2";

    $string =~ s/(.{64})/$1\n/g;
    note $string;
};

done_testing
