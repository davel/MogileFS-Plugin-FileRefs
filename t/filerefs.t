use strict;
use warnings;

use Test::More;
use MogileFS::Server;
use MogileFS::Test;

my $sto = eval { temp_store(); };
if (!$sto) {
        plan skip_all => "Can't create temporary test database: $@";
            exit 0;
}

my $store = Mgd::get_store;
isa_ok($store, 'MogileFS::Store');

done_testing();

