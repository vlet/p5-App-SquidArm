package App::SquidArm::DB;
use strict;
use warnings;
use DBI;
use Carp;

# SQLITE_MAX_VARIABLE_NUMBER default is 999
our $MAX_VARS = 999;

# SQLITE_MAX_COMPOUND_SELECT default is 500
our $MAX_COMPOUND = 500;

sub new {
    my ( $class, %opts ) = @_;
    bless {%opts}, $class;
}

sub create_tables {
    my $self = shift;
    my $dbh  = $self->db;

    $dbh->do(<<"    SQL");
        CREATE TABLE IF NOT EXISTS partitions (
            partition  CHAR(7),
            UNIQUE(partition)
        )
    SQL

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
        CREATE TABLE IF NOT EXISTS traf_stat(
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

    $self->suf;
}

sub create_access_table {

    my ( $self, $suf ) = @_;
    my $dbh = $self->db;
    croak "partition suffix not defined" unless defined $suf;

    $dbh->do(<<"    SQL");
        CREATE TABLE IF NOT EXISTS access_log_$suf (
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
    eval { $dbh->do( "INSERT INTO partitions VALUES (?)", undef, $suf ); };
}

sub suf {
    my ( $self, $ts ) = @_;

    my ( $year, $month ) = ( defined $ts ? localtime($ts) : localtime )[ 5, 4 ];
    my $suf = sprintf( "%d_%02d", $year + 1900, $month + 1 );
    if ( !exists $self->{suf} || $self->{suf} ne $suf ) {
        $self->{suf} = $suf;
        $self->create_access_table($suf);
    }
    $suf;
}

sub add_to_access {
    my ( $self, $values ) = @_;
    my $cnt = 15;
    while ( my @vars = splice( @$values, 0, int( $MAX_VARS / $cnt ) * $cnt ) ) {
        my $suf   = $self->suf( $vars[0] );
        my $query = "INSERT INTO access_log_$suf VALUES " . join ",",
          map { ( '(?' . ',?' x ( $cnt - 1 ) . ') ' ) }
          ( 1 .. int( @vars / $cnt ) );

        $self->db->do( $query, undef, @vars );
    }
}

sub add_to_stat {
    my ( $self, $values ) = @_;
    my $dbh = $self->db;
    my $cnt = 7;
    my ( $inserts, $updates );
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
    return ( $inserts, $updates );
}

sub add_to_hosts {
    my ( $self, $values ) = @_;
    my $dbh = $self->db;
    while ( my @vars = splice( @$values, 0, $MAX_COMPOUND ) ) {
        my $query = "INSERT OR IGNORE INTO hosts VALUES " . join ",",
          map { '(NULL, ?)' } ( 1 .. @vars );

        $self->db->do( $query, undef, @vars );
    }
}

sub add_to_users {
    my ( $self, $values ) = @_;
    my $dbh = $self->db;
    while ( my @vars = splice( @$values, 0, $MAX_COMPOUND ) ) {
        my $query = "INSERT OR IGNORE INTO users VALUES " . join ",",
          map { '(NULL, ?)' } ( 1 .. @vars );

        $self->db->do( $query, undef, @vars );
    }
}

sub get_hosts {
    shift->db->selectcol_arrayref("SELECT host FROM hosts");
}

sub get_users {
    shift->db->selectcol_arrayref("SELECT user FROM users");
}

sub db {
    my $self = shift;
    $self->{dbh} ||= DBI->connect( $self->connect_string ) or die $DBI::errstr;
}

sub connect_string {
    my $self = shift;
    my $d    = lc $self->{db_driver};
    if ( $d eq "sqlite" ) {
        (
            "dbi:SQLite:dbname=" . $self->{db_file},
            undef, undef,
            {
                RaiseError          => 1,
                AutoInactiveDestroy => 1
            }
        );
    }
    else {
        croak "unsupported DB type";
    }
}

sub traf_stat {
    my ( $self, $year, $month, $day ) = @_;
    my $dbh    = $self->db;
    my $format = '%Y-%m-%d';
    $format .= ' %H:%M' if defined $day;
    my $start =
      sprintf( "%d-%02d-%02d 00", $year, $month, ( defined $day ? $day : 1 ) );
    my $end =
      sprintf( "%d-%02d-%02d 24", $year, $month, ( defined $day ? $day : 31 ) );

    my $res = $dbh->selectall_arrayref( <<"    SQL", undef );
        SELECT
            strftime('$format',dt) as ts,
            SUM(hits), SUM(misses), SUM(reqs), SUM(denies)
        FROM traf_stat
        WHERE dt >= '$start' AND dt <= '$end'
        GROUP BY ts
    SQL

=cut

    my $res = $dbh
        ->selectall_arrayref(<<"    SQL", undef );
        SELECT  strftime('$format', unixts, 'unixepoch', 'localtime') as ts,
                SUM(s1) as hits, SUM(s2) as misses
        FROM (
            SELECT unixts, size as s1, 0 as s2
            FROM access_log_$part
            WHERE sqid_status LIKE '%HIT%'
          UNION
            SELECT unixts, 0 as s1, size as s2
            FROM access_log_$part
            WHERE sqid_status NOT LIKE '%HIT%'
        )
        $where
        GROUP BY ts
    SQL

=cut

}

sub traf_stat_user {
    my ( $self, $year, $month, $day ) = @_;
    my $dbh = $self->db;
    my $start =
      sprintf( "%d-%02d-%02d 00", $year, $month, ( defined $day ? $day : 1 ) );
    my $end =
      sprintf( "%d-%02d-%02d 24", $year, $month, ( defined $day ? $day : 31 ) );

    my $res = $dbh->selectall_arrayref( <<"    SQL", undef );
        SELECT
            users.user,
            SUM(hits), SUM(misses), SUM(reqs), SUM(denies)
        FROM traf_stat, users
        WHERE dt >= '$start' AND dt <= '$end' AND users.id=traf_stat.user
        GROUP BY users.user
    SQL

}

sub user_traf_stat {
    my ( $self, $user, $year, $month, $day ) = @_;
    my $dbh = $self->db;
    my $start =
      sprintf( "%d-%02d-%02d 00", $year, $month, ( defined $day ? $day : 1 ) );
    my $end =
      sprintf( "%d-%02d-%02d 24", $year, $month, ( defined $day ? $day : 31 ) );

    my $res = $dbh->selectall_arrayref( <<"    SQL", undef, $user );
        SELECT
            hosts.host,
            SUM(hits), SUM(misses), SUM(reqs), SUM(denies)
        FROM traf_stat, hosts, users
        WHERE dt >= '$start' AND dt <= '$end'
            AND users.user=? AND users.id=traf_stat.user
            AND hosts.id=traf_stat.host
        GROUP BY hosts.host
    SQL
}

sub begin {
    my $self = shift;
    $self->db->do("BEGIN");
}

sub end {
    my $self = shift;
    $self->db->do("END");
}

sub disconnect {
    my $self = shift;
    $self->db->disconnect;
}

1;
