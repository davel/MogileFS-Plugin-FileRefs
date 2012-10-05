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

lives_ok { $store->create_table("file_ref") };

open(my $null, "+>", "/dev/null") or die $!;
my $query = MogileFS::Worker::Query->new($null);
isa_ok($query, 'MogileFS::Worker::Query');

my $sent_to_parent;

no strict 'refs';
*MogileFS::Worker::Query::send_to_parent = sub {
    $sent_to_parent = $_[1];
};
use strict;

is(MogileFS::Plugin::FileRefs::add_file_ref($query, undef, "zz", "00001"), "0");
is($sent_to_parent, "ERR add_file_ref_fail add_file_ref_fail");

is(MogileFS::Plugin::FileRefs::add_file_ref($query, 1, "zz", "00001"), "1");
is($sent_to_parent, "OK made_new_ref=0");

is(MogileFS::Plugin::FileRefs::add_file_ref($query, 1, "zz", "00001"), "1");
is($sent_to_parent, "OK made_new_ref=1");

is(MogileFS::Plugin::FileRefs::add_file_ref($query, 1, "zz", "00001"), "1");
is($sent_to_parent, "OK made_new_ref=1");

is(MogileFS::Plugin::FileRefs::del_file_ref($query, 1, "zz", "00001"), "1");
is($sent_to_parent, "OK deleted_ref=1");

is(MogileFS::Plugin::FileRefs::del_file_ref($query, 1, "zz", "00001"), "1");
is($sent_to_parent, "OK deleted_ref=0");

done_testing();

