package MogileFS::Plugin::FileRefs;

use strict;
use warnings;

use MogileFS::Store;


our $VERSION = '0.03';
MogileFS::Store->add_extra_tables("file_ref");

sub load {
    MogileFS::register_worker_command('add_file_ref', \&add_file_ref) or die;

    MogileFS::register_worker_command('del_file_ref', \&del_file_ref) or die;

    MogileFS::register_worker_command('rename_if_no_refs', \&rename_if_no_refs) or die;

    MogileFS::register_worker_command('list_refs_for_dkey', \&list_refs_for_dkey) or die;
}

sub add_file_ref {
    my ($query, $args) = @_;
    my $dbh = Mgd::get_dbh();
    local $@;
    my $dmid = $query->check_domain($args) or return $query->err_line('domain_not_found');
    my $updated = eval { $dbh->do("REPLACE INTO file_ref (dmid, dkey, ref) VALUES (?, ?, ?)", {}, $dmid, $args->{arg1}, $args->{arg2}); };
    if ($@ || $dbh->err || $updated < 1) {
        return $query->err_line("add_file_ref_fail");
    }
    return $query->ok_line({made_new_ref => $updated>0 ? 1:0});
}

sub del_file_ref {
    my ($query, $args) = @_;
    my $dbh = Mgd::get_dbh();
    my $dmid = $query->check_domain($args) or return $query->err_line('domain_not_found');
    local $@;
    my $deleted = eval { $dbh->do("DELETE FROM file_ref WHERE dmid = ? AND dkey = ? AND ref = ?", {}, $dmid, $args->{arg1}, $args->{arg2}) };
    if ($@ || $dbh->err) {
        return $query->err_line("del_file_ref_fail");
    }
    return $query->ok_line({deleted_ref => $deleted>0 ? 1:0 });
}

# TODO - use a stored procedure.

sub rename_if_no_refs {
    my ($query, $args) = @_;
    my $dbh = Mgd::get_dbh();

    my $dmid = $query->check_domain($args) or return $query->err_line('domain_not_found');
    local $@;
    eval { $dbh->do("LOCK TABLES file_ref WRITE, file WRITE"); };
    if ($@ || $dbh->err) {
        eval { $dbh->do("UNLOCK TABLES"); };
        return $query->err_line("rename_if_no_refs_failed");
    }

    my ($count) = eval { $dbh->selectrow_array("SELECT COUNT(*) FROM file_ref WHERE dmid = ? AND dkey = ?", {}, $dmid, $args->{arg1}) };
    if ($@ || $dbh->err) {
        eval { $dbh->do("UNLOCK TABLES"); };
        return $query->err_line("rename_if_no_refs_failed");
    }

    if ($count != 0) {
        eval { $dbh->do("UNLOCK TABLES"); };
        return $query->ok_line({files_outstanding => $count});
    }

    my $updated = eval { $dbh->do("UPDATE file SET dkey = ? WHERE dmid = ? AND dkey = ?", {}, $args->{arg2}, $dmid, $args->{arg1}); };
    if ($@ || $dbh->err) {
        eval { $dbh->do("UNLOCK TABLES"); };
        return $query->err_line("rename_if_no_refs_failed");
    }
    eval { $dbh->do("UNLOCK TABLES"); };

    return $query->ok_line({files_outstanding => 0, updated => $updated+0});
}

sub list_refs_for_dkey {
    my ($query, $args) = @_;
    my $dmid = $query->check_domain($args) or return $query->err_line('domain_not_found');
    my $dbh = Mgd::get_dbh();
    my $result = eval {
        $dbh->selectcol_arrayref("SELECT ref FROM file_ref WHERE dmid = ? AND dkey = ?", {}, $dmid, $args->{arg1});
    };
    if ($@ || $dbh->err) {
        return $query->err_line("list_refs_for_dkey_failed");
    }
    my $i;
    return $query->ok_line({
        total => scalar(@$result),
        map { "ref_".$i++ => $_ } @{ $result }
    });

}

sub update_schema {
    my $store = Mgd::get_store();
    my $dbh = $store->dbh();
    $dbh->do($store->filter_create_sql(TABLE_fileref()))
        or die "Failed to create table file_ref: ". $dbh->errstr;

    return;
}

{
    package MogileFS::Store;

    sub TABLE_file_ref {
        q{CREATE TABLE `file_ref` (
      `dmid` SMALLINT UNSIGNED NOT NULL,
      `dkey` varchar(255) DEFAULT NULL,
      `ref`  varchar(255) DEFAULT NULL,
      UNIQUE KEY `i_unique` (`dmid`,`dkey`,`ref`)
    );
        };
    }
}

1;
__END__

=head1 NAME

MogileFS::Plugin::FileRefs - MogileFS extension

=head1 DESCRIPTION

This module provides MogileFS with additional functionality to keep track of
multiple uses of an individual dkey.  This may be useful for implementing
de-duplication in your application.


=head2 Mogile commands

=over

=item add_file_ref

Takes domain id, dkey, and your reference as arguments.

Creates an association from a key to your reference.

=item del_file_ref

Takes domain id, dkey, and your reference as arguments.

Deletes an association from a key to your reference.

=item rename_if_no_refs

Takes domain id, old dkey and new dkey.

Atomically renames a dkey provided there are no references to it.

=item list_refs_for_dkey

Takes domain id, dkey.

Lists all the references against a dkey.

=back

=head1 SEE ALSO

L<MogileFS::Server>

=head1 AUTHOR

Dave Lambley, E<lt>davel@state51.co.ukE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012 Oscar Music and Media

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.10.1 or,
at your option, any later version of Perl 5 you may have available.


=cut
