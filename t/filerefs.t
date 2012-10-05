use strict;
use warnings;

use Test::More;
use MogileFS::Server;
use MogileFS::Test;
use MogileFS::Plugin::FileRefs;
use MogileFS::Worker::Query;
use Test::Exception;

my $sto = eval { temp_store(); };
if (!$sto) {
        plan skip_all => "Can't create temporary test database: $@";
            exit 0;
}

my $store = Mgd::get_store;
isa_ok($store, 'MogileFS::Store');

lives_ok { MogileFS::Plugin::FileRefs::update_schema() };

open(my $null, "+>", "/dev/null") or die $!;
my $query = MogileFS::Worker::Query->new($null);
isa_ok($query, 'MogileFS::Worker::Query');

is(MogileFS::Plugin::FileRefs::add_file_ref($query, undef, "zz", "00001"), "0");
is(MogileFS::Plugin::FileRefs::add_file_ref($query, 1, "zz", "00001"), "1");
is(MogileFS::Plugin::FileRefs::add_file_ref($query, 1, "zz", "00001"), "1");

done_testing();

