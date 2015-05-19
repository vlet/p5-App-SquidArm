package    # Test
  App::SquidArm::Test;
use strict;
use warnings;

sub run {
    my ( $fh, $pipe ) = @_;
    print $pipe "hi\n";
}

1
