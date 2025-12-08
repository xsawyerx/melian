use strict;
use warnings;
use FindBin ();
use lib "$FindBin::Bin";
use Dumbbench;
use Redis;
use List::Util qw(first);
use Melian;

use constant PACKED_ID => pack('V', 5);

my $bench = Dumbbench->new(
    'target_rel_precision' => 0.005, # seek ~0.5%
    'initial_runs'         => 20,    # the higher the more reliable
);

my $redis = Redis->new('server' => '127.0.0.1:6379');

my $melian = Melian->new(
    'dsn'         => 'unix:///tmp/melian.sock',
    'schema_spec' => 'table1#0|60|id#0:int,table2#1|60|id#0:int;hostname#1:string',
    'timeout'     => 1,
);

my $conn = Melian->create_connection(
    'dsn' => 'unix:///tmp/melian.sock',
);

$bench->add_instances(
    Dumbbench::Instance::PerlSub->new(
        'name' => 'Melian',
        'code' => sub {
            for (1 .. 1e4) {
                Melian::fetch_raw_with($conn, 0, 0, PACKED_ID() );
            }
        }
    ),

    Dumbbench::Instance::PerlSub->new(
        'name' => 'Melian OO',
        'code' => sub {
            for (1 .. 1e4) {
                $melian->fetch_raw( 0, 0, PACKED_ID() );
            }
        }
    ),

    Dumbbench::Instance::PerlSub->new(
        'name' => 'Redis',
        'code' => sub {
            for (1 .. 1e4) {
                $redis->get('t1:id:5');
            }
        }
    ),
);

$bench->run;
$bench->report;

__END__

Melian: Ran 29 iterations (9 outliers).
Melian: Rounded run time per iteration (seconds): 9.7342e-02 +/- 5.7e-05 (0.1%)
Melian OO: Ran 25 iterations (3 outliers).
Melian OO: Rounded run time per iteration (seconds): 9.660e-02 +/- 3.1e-04 (0.3%)
Redis: Ran 28 iterations (8 outliers).
Redis: Rounded run time per iteration (seconds): 2.5560e-01 +/- 2.1e-04 (0.1%)
