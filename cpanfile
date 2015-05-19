requires 'perl', '5.008001';
requires 'AnyEvent', '7.02';
requires 'AnyEvent::Fork';
requires 'DBI','1.622';
requires 'DBD::SQLite', '1.37';
requires 'Raisin', '0.61';
requires 'Types::Standard';
requires 'Carp';
requires 'File::Spec';
requires 'JSON';
requires 'MIME::Base64';
requires 'parent';
requires 'Proc::Daemon';
requires 'DateTime';
requires 'File::ShareDir';
requires 'Plack';
requires 'Plack::Middleware::Static';

on 'test' => sub {
    requires 'Test::More', '0.98';
    requires 'Test::TCP', '1.17';
};

