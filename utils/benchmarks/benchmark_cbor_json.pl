use strict;
use warnings;
use JSON::XS qw< encode_json decode_json >;
use CBOR::XS qw< encode_cbor decode_cbor >;
use Dumbbench;

my $struct = {
    'active'      => 1,
    'category'    => 'alpha',
    'created_at'  => '2025-10-30 14:26:47',
    'description' => 'Mock description for item 5',
    'id'          => 5,
    'name'        => 'item_5',
    'updated_at'  => '2025-11-04 14:26:47',
    'value'       => 'VAL_0005',
};

my $struct_json = encode_json($struct);
my $struct_cbor = encode_cbor($struct);

my $bench = Dumbbench->new(
    target_rel_precision => 0.005, # seek ~0.5%
    initial_runs         => 20,    # the higher the more reliable
);

my $max = 1e6;
$bench->add_instances(
    Dumbbench::Instance::PerlSub->new(
        'name' => 'JSON encode',
        'code' => sub {
            for ( 1 .. $max ) {
                encode_json($struct);
            }
        }
    ),

    Dumbbench::Instance::PerlSub->new(
        'name' => 'JSON decode',
        'code' => sub {
            for ( 1 .. $max ) {
                decode_json($struct_json);
            }
        }
    ),

    Dumbbench::Instance::PerlSub->new(
        'name' => 'CBOR encode',
        'code' => sub {
            for ( 1 .. $max ) {
                encode_cbor($struct);
            }
        }
    ),

    Dumbbench::Instance::PerlSub->new(
        'name' => 'CBOR decode',
        'code' => sub {
            for ( 1 .. $max ) {
                decode_cbor($struct_cbor);
            }
        }
    ),
);

$bench->run;
$bench->report;


__END__

JSON encode: Rounded run time per iteration (seconds): 3.5576e-01 +/- 3.8e-04 (0.1%)
JSON decode: Rounded run time per iteration (seconds): 6.0477e-01 +/- 5.7e-04 (0.1%)
CBOR encode: Rounded run time per iteration (seconds): 4.3528e-01 +/- 7.5e-04 (0.2%)
CBOR decode: Rounded run time per iteration (seconds): 5.5316e-01 +/- 3.6e-04 (0.1%)

