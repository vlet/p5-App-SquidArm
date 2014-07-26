requires 'perl', '5.008001';
requires 'AnyEvent', '7.02';
requires 'AnyEvent::AIO', '1.1';
requires 'IO::AIO', '4.18';
requires 'DBI','1.622';
requires 'DBD::SQLite', '1.37';
requires 'Dancer', '1.3110';
requires 'Carp';
requires 'File::Spec';
requires 'JSON';
requires 'MIME::Base64';
requires 'parent';
requires 'Proc::Daemon';

on 'test' => sub {
    requires 'Test::More', '0.98';
    requires 'Test::TCP', '1.17';
    requires 'Text::Diff', '1.41';
};

