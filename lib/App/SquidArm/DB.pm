package App::SquidArm::DB;
use strict;
use warnings;
use DBI;
use Carp;
use File::Spec::Functions;
use DateTime;
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

sub encode_tz {
    my $self = shift;
    my $tz   = lc( $self->{tz} );
    $tz =~ s{/}{__}g;
    $tz;
}

sub create_tables {
    my $self = shift;
    my $dbh  = $self->db(STAT_DB);
    my $tz   = $self->encode_tz;

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
        CREATE TABLE IF NOT EXISTS traf_stat_$tz (
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

    $dbh->do(<<"    SQL");
        CREATE TABLE IF NOT EXISTS traf_gstat_$tz (
            dt      TIMESTAMP NOT NULL,
            user    INTEGER NOT NULL,
            misses  INTEGER NOT NULL,
            hits    INTEGER NOT NULL,
            reqs    INTEGER NOT NULL,
            denies  INTEGER NOT NULL,
            FOREIGN KEY(user) REFERENCES users(id),
            UNIQUE(dt,user)
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
    my $tz  = $self->encode_tz;
    my $cnt = 7;
    my ( $inserts, $updates ) = ( 0, 0 );
    $dbh->do("BEGIN");
    while ( my @vars = splice( @$values, 0, $cnt ) ) {

        eval {
            $dbh->do( <<"            SQL", undef, @vars );
                INSERT INTO traf_stat_$tz VALUES (?,(
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
                UPDATE traf_stat_$tz
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

sub add_to_gstat {
    my ( $self, $values ) = @_;
    my $dbh = $self->db(STAT_DB);
    my $tz  = $self->encode_tz;
    my $cnt = 6;
    my ( $inserts, $updates ) = ( 0, 0 );
    $dbh->do("BEGIN");
    while ( my @vars = splice( @$values, 0, $cnt ) ) {
        my $ret = $dbh->do( <<"        SQL", undef, @vars );
            UPDATE traf_gstat_$tz
            SET misses=misses+?3, hits=hits+?4, reqs=reqs+?5, denies=denies+?6
            WHERE dt=?1 AND user=(
                SELECT id
                FROM users
                WHERE user=?2
            )
        SQL
        $updates++ if $ret;
        eval {
            $dbh->do( <<"            SQL", undef, @vars );
                INSERT INTO traf_gstat_$tz VALUES (?,(
                    SELECT id
                    FROM users
                    WHERE user=?
                ),?,?,?,?)
            SQL
            $inserts++ if $ret;
        } if $ret == 0;
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
            undef, undef, { RaiseError => 1, PrintError => 0 }
        );
    }
    else {
        croak "unsupported DB type";
    }
}

sub traf_stat_ym {
    my ( $self, $year, $month ) = @_;
    my $dbh = $self->db(STAT_DB);

    my $dt = DateTime->new(
        year      => $year,
        month     => $month,
        time_zone => $self->{tz}
    );
    my $start = $dt->epoch;
    my $end = $dt->add( months => 1 )->epoch;

    my @common = ( $dbh, $self->encode_tz, $year, $month );
    my $day = 1;

    # Check for DST or other changes in this month
    if ( ( $end - $start ) % 86400 ) {
        my $res = [];
        my ( $s, $e ) = ( $start, $start );
        $dt = DateTime->from_epoch( epoch => $start, time_zone => $self->{tz} );

        # Itarate over each day of month to find out 3 periods:
        #   1. days before DST change,
        #   2. day of DST change,
        #   3. days after DST change
        while ( $e < $end ) {
            $e = $dt->add( days => 1 )->epoch;
            next unless ( $e - $s ) % 86400;
            if ( $s != $start ) {
                push @$res, @{ _traf_stat_ym( @common, $start, $s, $day ) };
                $day = $dt->day - 1;
            }
            push @$res, @{ _traf_stat_ym( @common, $s, $e, $day, $e - $s ) };
            $start = $e;
            $day   = $dt->day;
        }
        continue {
            $s = $e;
        }
        push @$res, @{ _traf_stat_ym( @common, $start, $end, $day ) }
          if $start != $end;
        $res;
    }
    else {
        _traf_stat_ym( @common, $start, $end, $day );
    }
}

sub _traf_stat_ym {
    my ( $dbh, $tz, $year, $month, $start, $end, $day, $delta ) = @_;
    $delta ||= 86400;

    my $ym = sprintf "%d-%02d-", $year, $month;
    $dbh->selectall_arrayref( <<"    SQL", undef );
        SELECT
            '$ym' || 
            substr( '00' || cast( $day + (dt - $start)/$delta as string ), -2, 2 )
            as ts,
            SUM(hits), SUM(misses), SUM(reqs), SUM(denies)
        FROM traf_gstat_$tz
        WHERE dt >= $start AND dt < $end
        GROUP BY ts
    SQL
}

sub traf_stat_all_users_ymd {
    my ( $self, $year, $month, $day ) = @_;
    my $dbh = $self->db(STAT_DB);

    my $dt = DateTime->new(
        year  => $year,
        month => $month,
        defined $day ? ( day => $day ) : (),
        time_zone => $self->{tz}
    );

    my $start = $dt->epoch;
    my $end = $dt->add( ( defined $day ? 'days' : 'months' ) => 1 )->epoch;

    my $tz = $self->encode_tz;

    $dbh->selectall_arrayref( <<"    SQL", undef );
        SELECT
            users.user,
            SUM(hits), SUM(misses), SUM(reqs), SUM(denies)
        FROM traf_gstat_$tz, users
        WHERE   dt >= $start
            AND dt <  $end
            AND users.id=traf_gstat_$tz.user
        GROUP BY users.user
    SQL
}

sub traf_stat_user_ymd {
    my ( $self, $user, $year, $month, $day ) = @_;
    my $dbh = $self->db(STAT_DB);

    my $dt = DateTime->new(
        year  => $year,
        month => $month,
        defined $day ? ( day => $day ) : (),
        time_zone => $self->{tz}
    );

    my $start = $dt->epoch;
    my $end = $dt->add( ( defined $day ? 'days' : 'months' ) => 1 )->epoch;

    my $tz = $self->encode_tz;

    $dbh->selectall_arrayref( <<"    SQL", undef, $user );
        SELECT
            hosts.host,
            SUM(hits), SUM(misses), SUM(reqs), SUM(denies)
        FROM traf_stat_$tz, hosts, users
        WHERE   dt >= $start
            AND dt <  $end
            AND users.user=? AND users.id=traf_stat_$tz.user
            AND hosts.id=traf_stat_$tz.host
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
