package MogileFS::Plugin::FileRefs;

use strict;
use warnings;

use MogileFS::Store;


our $VERSION = '0.01';
MogileFS::Store::add_extra_tables("file_ref");

sub load {
    MogileFS::register_worker_command('add_file_ref', \&add_file_ref) or die;

    MogileFS::register_worker_command('del_file_ref', \&del_file_ref) or die;

    MogileFS::register_worker_command('rename_if_no_refs', \&rename_if_no_refs) or die;
}

sub add_file_ref {
    my ($query, $dmid, $dkey, $ref) = @_;
    my $dbh = Mgd::get_dbh();
    local $@;
    my $updated = eval { $dbh->do("REPLACE INTO file_ref (dmid, dkey, ref) VALUES (?, ?, ?)", {}, $dmid, $dkey, $ref); };
    if ($@ || $dbh->err || $updated < 1) {
        return $query->err_line("add_file_ref_fail");
    }

    return $query->ok_line({made_new_ref => $updated>1 ? 1:0});
}

sub del_file_ref {
    my ($query, $dmid, $dkey, $ref) = @_;
    my $dbh = Mgd::get_dbh();
    local $@;
    my $deleted = eval { $dbh->do("DELETE FROM file_ref WHERE dmid = ? AND dkey = ? AND ref = ?", {}, $dmid, $dkey, $ref) };
    if ($@ || $dbh->err) {
        warn $@;
        return $query->err_line("del_file_ref_fail");
    }
    return $query->ok_line({deleted_ref => $deleted>0 ? 1:0 });
}

sub rename_if_no_refs {
    my ($query, $dmid, $dkey, $new_dkey) = @_;
    my $dbh = Mgd::get_dbh();

    local $@;
    eval { $dbh->do("LOCK TABLES file_ref WRITE"); };
    if ($@ || $dbh->err) {
        eval { $dbh->do("UNLOCK TABLES"); };
        return $query->err_line("rename_if_no_refs_failed");
    }

    my ($count) = eval { $dbh->selectrow_array("SELECT COUNT(*) FROM file_ref WHERE dmid = ? AND dkey = ?", {}, $dmid, $dkey) };
    if ($@ || $dbh->err) {
        eval { $dbh->do("UNLOCK TABLES"); };
        return $query->err_line("rename_if_no_refs_failed");
    }

    if ($count != 0) {
        eval { $dbh->do("UNLOCK TABLES"); };
        return $query->ok_line({files_outstanding => $count});
    }

    my $updated = eval { $dbh->do("UPDATE file SET dkey = ? WHERE dmid = ? AND dkey = ?", {}, $new_dkey, $dmid, $dkey); };
    if ($@ || $dbh->err) {
        eval { $dbh->do("UNLOCK TABLES"); };
        return $query->err_line("rename_if_no_refs_failed");
    }
    eval { $dbh->do("UNLOCK TABLES"); };

    return $query->ok_line({files_outstanding => 0, updated => $updated});
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
# Below is stub documentation for your module. You'd better edit it!

=head1 NAME

MogileFS::Plugin::FileRefs - Perl extension for blah blah blah

=head1 SYNOPSIS

  use MogileFS::Plugin::FileRefs;
  blah blah blah

=head1 DESCRIPTION

Stub documentation for MogileFS::Plugin::FileRefs, created by h2xs. It looks like the
author of the extension was negligent enough to leave the stub
unedited.

Blah blah blah.

=head2 EXPORT

None by default.



=head1 SEE ALSO

Mention other useful documentation such as the documentation of
related modules or operating system documentation (such as man pages
in UNIX), or any relevant external documentation such as RFCs or
standards.

If you have a mailing list set up for your module, mention it here.

If you have a web site set up for your module, mention it here.

=head1 AUTHOR

Dave Lambley, E<lt>davel@state51.co.ukE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012 Oscar Music and Media

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.10.1 or,
at your option, any later version of Perl 5 you may have available.


=cut
