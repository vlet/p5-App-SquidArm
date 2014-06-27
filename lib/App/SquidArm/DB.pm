package App::SquidArm::DB;
use strict;
use warnings;
use DBI;
use Carp;

# maximum inserted rows
# depends on SQLITE_MAX_VARIABLE_NUMBER which default is 999
our $MAX_INSERT = 60;

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
        CREATE TABLE IF NOT EXISTS traf_stat(
            dt      TIMESTAMP NOT NULL,
            user    TEXT NOT NULL,
            misses  INT NOT NULL,
            hits    INT NOT NULL,
            reqs    INT NOT NULL,
            UNIQUE(dt,user)
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
    my $cnt   = 15;
    my $suf   = $self->suf( $values->[0] );
    my $query = "INSERT INTO access_log_$suf VALUES " . join ",",
      map { ( '(?' . ',?' x ( $cnt - 1 ) . ') ' ) }
      ( 1 .. int( @$values / $cnt ) );

    $self->db->do( $query, undef, @$values );
}

sub add_to_stat {
    my ( $self, $values ) = @_;
    my $dbh = $self->db;
    my $cnt = 5;
    while ( my @vars = splice( @$values, 0, $cnt ) ) {

        my $ret = $dbh->do( <<"        SQL", undef, @vars );
            UPDATE traf_stat
            SET misses=misses+?3, hits=hits+?4, reqs=reqs+?5
            WHERE dt=?1 AND user=?2
        SQL

        eval {
            $ret = $dbh->do( <<"            SQL", undef, @vars );
                INSERT INTO traf_stat VALUES (?,?,?,?,?)
            SQL
        } if $ret == 0;
    }
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
            SUM(hits), SUM(misses), SUM(reqs)
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

sub user_traf_stat {
    my ( $self, $year, $month, $day ) = @_;
    my $dbh = $self->db;
    my $start =
      sprintf( "%d-%02d-%02d 00", $year, $month, ( defined $day ? $day : 1 ) );
    my $end =
      sprintf( "%d-%02d-%02d 24", $year, $month, ( defined $day ? $day : 31 ) );

    my $res = $dbh->selectall_arrayref( <<"    SQL", undef );
        SELECT
            user,
            SUM(hits), SUM(misses), SUM(reqs)
        FROM traf_stat
        WHERE dt >= '$start' AND dt <= '$end'
        GROUP BY user
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
