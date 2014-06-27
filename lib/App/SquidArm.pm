package App::SquidArm;
use 5.008005;
use strict;
use warnings;

our $VERSION = "0.01";

1;
__END__

=encoding utf-8

=head1 NAME

App::SquidArm - collection of tools for Squid proxy server: tcp log daemon, log
analyser, and web interface with proxy usage statistic.

=head1 SYNOPSIS

    # start tcp log daemon
    $ sarm_log -c sarm.conf

    # start web interface
    $ sarm_web

=head1 DESCRIPTION

App::SquidArm is a complicated mess of code for apparenly simple task:
collecting and parsing Squid proxy server's log.

B<sarm_log> is a daemon that listen on some tcp port for incoming connections
of one or several squid proxy servers. It receive plain squid log, parse it on
a fly, store parsed data in database and raw data in a plain file.

B<sarm_web> is a web-server with web-interface for proxy administrator that
shows analysed data of proxy server usage: user reports (per day, month, year),
total statistics, etc.

Development status: in progress...

Perfomance: for a current moment it can parse and store data in a SQLite
database with a rate ~350 KB/s of logs on my old Pentuim IV 3GHz CPU with slow
SATA drive.

=head1 LICENSE

Copyright (C) Vladimir Lettiev.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

Vladimir Lettiev E<lt>thecrux@gmail.comE<gt>

=cut

