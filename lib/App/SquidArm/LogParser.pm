package App::SquidArm::LogParser;
use strict;
use warnings;
use AnyEvent;
use AnyEvent::Handle;
use parent qw(App::SquidArm::Helper);

sub run {
    my ( $fh, $parser_pipe1, $parser_pipe2, $pipe_log, $conf ) = @_;
    my $self = App::SquidArm::LogParser->new(
        conf         => $conf,
        master_fh    => $fh,
        parser_pipes => [ $parser_pipe1, $parser_pipe2 ],
        log_pipe     => $pipe_log,
    );

    my $w = AE::cv;
    #<<< dear perltidy, please don't ruin this nice formatting
    $self
        ->init_logging
        ->init_debugging
        ->handle_parser_pipes
        ->handle_log_pipe
        ->handle_signals($w)
        ->handle_master_pipe($w)
        ;
    #>>>
    $w->recv;
}

sub handle_parser_pipes {
    my $self = shift;
    for my $pipe ( @{ $self->{parser_pipes} } ) {
        push @{ $self->{pp_h} }, AnyEvent::Handle->new(
            fh       => $pipe,
            on_error => sub {
                AE::log error => $_[2];
                $_[0]->destroy;
            }
        );
    }
    $self;
}

sub handle_log_pipe {
    my $self   = shift;
    my $readed = 0;

    my $handle;
    $handle = AnyEvent::Handle->new(
        fh       => $self->{log_pipe},
        on_error => sub {
            AE::log error => $_[2];
            $_[0]->destroy;
        },
        on_eof => sub {
            $handle->destroy;
            AE::log info => "Done";
        },
        on_read => sub {
            return unless length $handle->{rbuf};
            AE::log debug => "parser got " . length( $handle->{rbuf} );
            my $len;
            my @records;
            for ( 1 .. 2 ) {
                $len = _parser( \$handle->{rbuf}, \@records );
                last if defined $len && $len > 0;

                # first line failed in parser
                # find first \n position
                my $i = index( $handle->{rbuf}, "\012" );
                if ( $i == -1 ) {
                    return;
                }
                my $junk = substr( $handle->{rbuf}, 0, $i + 1, '' );
                AE::log error => "parser find junk at start position:\n"
                  . $junk;
            }
            if ( !defined $len || $len == 0 ) {
                AE::log error => 'parser: malformed input';
                return;
            }
            substr $handle->{rbuf}, 0, $len, '';
            $readed += $len;
            AE::log debug => "parser read $len (total $readed)";
            AE::log debug => "send records to db pipe";
            for ( @{ $self->{pp_h} } ) {
                $_->push_write( storable => \@records );
            }
        }
    );

    $self;
}

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

sub _parser {
    my ( $buf_ref, $records, $stats ) = @_;
    my @data;
    pos($$buf_ref) = 0;

    while ( $$buf_ref =~ /$squid_log_re/gc ) {

        @data = (
            $1, $2, $3,               $4, $5, $6, $7, $8,
            $9, undef, # URL -> HOST, URI
            defined $10 ? lc($10) : undef,
            defined $11 ? lc($11) : undef,
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
    }
    my $i = pos $$buf_ref;
    return $i;
}

1
