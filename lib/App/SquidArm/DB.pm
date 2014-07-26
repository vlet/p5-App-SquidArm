package App::SquidArm::DB;
use strict;
use warnings;
use DBI;
use Carp;
use File::Spec::Functions;
use constant {
    STAT_DB   => 'stat',
    ACCESS_DB => 'access_log',
};

# SQLITE_MAX_VARIABLE_NUMBER default is 999
our $MAX_VARS = 999;

# SQLITE_MAX_COMPOUND_SELECT default is 500
our $MAX_COMPOUND = 500;

sub new {
    my ( $class, %opts ) = @_;
    bless { %opts, dbh => {} }, $class;
}

sub create_tables {
    my $self = shift;
    my $dbh  = $self->db(STAT_DB);

    $dbh->do(<<"    SQL");
        CREATE TABLE IF NOT EXISTS hosts (
            id      INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            host    TEXT,
            UNIQUE(host)
        )
    SQL

    $dbh->do(<<"    SQL");
        CREATE TABLE IF NOT EXISTS users (
            id      INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            user    TEXT,
            UNIQUE(user)
        )
    SQL

    $dbh->do(<<"    SQL");
        CREATE TABLE IF NOT EXISTS traf_stat (
            dt      TIMESTAMP NOT NULL,
            user    INTEGER NOT NULL,
            host    INTEGER NOT NULL,
            misses  INTEGER NOT NULL,
            hits    INTEGER NOT NULL,
            reqs    INTEGER NOT NULL,
            denies  INTEGER NOT NULL,
            FOREIGN KEY(host) REFERENCES hosts(id),
            FOREIGN KEY(user) REFERENCES users(id),
            UNIQUE(dt,user,host)
        )
    SQL

    $self->dbname;
}

sub create_access_table {

    my ( $self, $dbname ) = @_;
    croak "name of partition db not defined" unless defined $dbname;
    my $dbh = $self->db($dbname);

    $dbh->do(<<"    SQL");
        CREATE TABLE IF NOT EXISTS access_log (
            unixts      INT NOT NULL,
            msec        INT NOT NULL,
            resptime    INT NOT NULL,
            ip          VARCHAR(45),
            sqid_status VARCHAR(50) NOT NULL,
            clnt_status INT NOT NULL,
            size        INT NOT NULL,
            method      VARCHAR(10) NOT NULL,
            host        TEXT,
            uri         TEXT,
            username    TEXT,
            realm       TEXT,
            hier_status VARCHAR(50) NOT NULL,
            srv_ip      VARCHAR(45),
            mime        TEXT
        )
    SQL
}

sub dbname {
    my ( $self, $ts ) = @_;

    my ( $year, $month ) = ( defined $ts ? gmtime($ts) : gmtime )[ 5, 4 ];
    my $dbname = sprintf( ACCESS_DB . "_%d_%02d", $year + 1900, $month + 1 );
    if ( !$self->{dbh}->{$dbname} || $self->{dbname} ne $dbname ) {
        $self->{dbname} = $dbname;
        $self->create_access_table($dbname);
    }
    $dbname;
}

sub add_to_access {
    my ( $self, $values ) = @_;
    my $cnt    = 15;
    my $dbname = $self->dbname( $values->[0] );
    my $dbh    = $self->db($dbname);
    $dbh->do("BEGIN");

    while ( my @vars = splice( @$values, 0, int( $MAX_VARS / $cnt ) * $cnt ) ) {
        my $query = "INSERT INTO access_log VALUES " . join ",",
          map { ( '(?' . ',?' x ( $cnt - 1 ) . ') ' ) }
          ( 1 .. int( @vars / $cnt ) );

        my $check_dbname = $self->dbname( $vars[0] );
        if ( $check_dbname ne $dbname ) {
            $dbh->do("END");
            undef $dbh;
            $self->disconnect($dbname);

            $dbname = $check_dbname;
            $dbh    = $self->db($dbname);
            $dbh->do("BEGIN");
        }
        $dbh->do( $query, undef, @vars );
    }

    $dbh->do("END");
}

sub add_to_stat {
    my ( $self, $values ) = @_;
    my $dbh = $self->db(STAT_DB);
    my $cnt = 7;
    my ( $inserts, $updates ) = ( 0, 0 );
    $dbh->do("BEGIN");
    while ( my @vars = splice( @$values, 0, $cnt ) ) {

        eval {
            $dbh->do( <<"            SQL", undef, @vars );
                INSERT INTO traf_stat VALUES (?,(
                    SELECT id
                    FROM users
                    WHERE user=?
                ),(
                    SELECT id
                    FROM hosts
                    WHERE host=?
                ),?,?,?,?)
            SQL
        };
        if ($@) {
            $dbh->do( <<"            SQL", undef, @vars );
                UPDATE traf_stat
                SET misses=misses+?4, hits=hits+?5, reqs=reqs+?6, denies=denies+?7
                WHERE dt=?1 AND user=(
                    SELECT id
                    FROM users
                    WHERE user=?2
                ) AND host=(
                    SELECT id
                    FROM hosts
                    WHERE host=?3
                )
            SQL
            $updates++;
        }
        else {
            $inserts++;
        }
    }
    $dbh->do("END");
    return ( $inserts, $updates );
}

sub add_to_hosts {
    my ( $self, $values ) = @_;
    my $dbh = $self->db(STAT_DB);
    $dbh->do("BEGIN");
    while ( my @vars = splice( @$values, 0, $MAX_COMPOUND ) ) {
        my $query = "INSERT OR IGNORE INTO hosts VALUES " . join ",",
          map { '(NULL, ?)' } ( 1 .. @vars );

        $dbh->do( $query, undef, @vars );
    }
    $dbh->do("END");
}

sub add_to_users {
    my ( $self, $values ) = @_;
    my $dbh = $self->db(STAT_DB);
    $dbh->do("BEGIN");
    while ( my @vars = splice( @$values, 0, $MAX_COMPOUND ) ) {
        my $query = "INSERT OR IGNORE INTO users VALUES " . join ",",
          map { '(NULL, ?)' } ( 1 .. @vars );

        $dbh->do( $query, undef, @vars );
    }
    $dbh->do("END");
}

sub get_hosts {
    shift->db(STAT_DB)->selectcol_arrayref("SELECT host FROM hosts");
}

sub get_users {
    shift->db(STAT_DB)->selectcol_arrayref("SELECT user FROM users");
}

sub db {
    my ( $self, $db ) = @_;
    croak 'db name name not defined' unless $db;
    $self->{dbh}->{$db} ||= DBI->connect( $self->connect_string($db) )
      or die $DBI::errstr;
}

sub connect_string {
    my ( $self, $db ) = @_;
    my $d = lc $self->{db_driver};
    if ( $d eq "sqlite" ) {
        (
            'dbi:SQLite:dbname=' . catfile( $self->{db_dir}, $db . '.db' ),
            undef, undef, { RaiseError => 1, }
        );
    }
    else {
        croak "unsupported DB type";
    }
}

sub traf_stat {
    my ( $self, $year, $month, $day ) = @_;
    my $dbh    = $self->db(STAT_DB);
    my $format = '%Y-%m-%d';
    $format .= ' %H:%M' if defined $day;
    my $start = sprintf( "%d-%02d-%02d 00:00",
        $year, $month, ( defined $day ? $day : 1 ) );
    my $end = sprintf( "%d-%02d-%02d 24:00",
        $year, $month, ( defined $day ? $day : 31 ) );

    my $res = $dbh->selectall_arrayref( <<"    SQL", undef );
        SELECT
            strftime( '$format', dt, 'localtime' ) as ts,
            SUM(hits), SUM(misses), SUM(reqs), SUM(denies)
        FROM traf_stat
        WHERE   dt >= strftime('%Y-%m-%d %H:%M', '$start', 'utc')
            AND dt <= strftime('%Y-%m-%d %H:%M', '$end',   'utc')
        GROUP BY ts
    SQL
}

sub traf_stat_user {
    my ( $self, $year, $month, $day ) = @_;
    my $dbh   = $self->db(STAT_DB);
    my $start = sprintf( "%d-%02d-%02d 00:00",
        $year, $month, ( defined $day ? $day : 1 ) );
    my $end = sprintf( "%d-%02d-%02d 24:00",
        $year, $month, ( defined $day ? $day : 31 ) );

    my $res = $dbh->selectall_arrayref( <<"    SQL", undef );
        SELECT
            users.user,
            SUM(hits), SUM(misses), SUM(reqs), SUM(denies)
        FROM traf_stat, users
        WHERE   dt >= strftime('%Y-%m-%d %H:%M', '$start', 'utc')
            AND dt <= strftime('%Y-%m-%d %H:%M', '$end',   'utc')
            AND users.id=traf_stat.user
        GROUP BY users.user
    SQL

}

sub user_traf_stat {
    my ( $self, $user, $year, $month, $day ) = @_;
    my $dbh   = $self->db(STAT_DB);
    my $start = sprintf( "%d-%02d-%02d 00:00",
        $year, $month, ( defined $day ? $day : 1 ) );
    my $end = sprintf( "%d-%02d-%02d 24:00",
        $year, $month, ( defined $day ? $day : 31 ) );

    my $res = $dbh->selectall_arrayref( <<"    SQL", undef, $user );
        SELECT
            hosts.host,
            SUM(hits), SUM(misses), SUM(reqs), SUM(denies)
        FROM traf_stat, hosts, users
        WHERE   dt >= strftime('%Y-%m-%d %H:%M', '$start', 'utc')
            AND dt <= strftime('%Y-%m-%d %H:%M', '$end',   'utc')
            AND users.user=? AND users.id=traf_stat.user
            AND hosts.id=traf_stat.host
        GROUP BY hosts.host
    SQL
}

sub disconnect {
    my ( $self, $db ) = @_;
    croak 'no db name defined' unless $db;
    delete $self->{dbh}->{$db};
    ();
}

1;
