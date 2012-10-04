package MogileFS::Plugin::FileRefs;

use strict;
use warnings;

our $VERSION = '0.01';

sub load {
    MogileFS::register_worker_command('add_file_ref', sub {
        my ($query, $dmid, $dkey, $ref) = @_;
        my $dbh = Mgd::get_dbh();
        local $@;
        my $updated = eval { $dbh->do("REPLACE INTO file_ref (dmid, dkey, ref) VALUES (?, ?, ?)", {}, $dmid, $dkey, $ref); };
        if ($@ || $dbh->err) {
            return $query->err_line("add_file_ref_fail");
        }

        return $query->ok_line({made_new_ref => $updated});
    }) or die;

    MogileFS::register_worker_command('del_file_ref', sub {
        my ($query, $dmid, $dkey, $ref) = @_;
        my $dbh = Mgd::get_dbh();
        local $@;
        my $deleted = eval { $dbh->do("DELETE file_ref WHERE dmid = ? AND dkey = ? AND ref = ?", {}, $dmid, $dkey, $ref) };
        if ($@ || $dbh->err) {
            return $query->err_line("del_file_ref_fail");
        }
        return $query->ok_line({deleted_ref => $deleted });
    }) or die;

    MogileFS::register_worker_command('rename_if_no_refs', sub {
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
    }) or die;
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
