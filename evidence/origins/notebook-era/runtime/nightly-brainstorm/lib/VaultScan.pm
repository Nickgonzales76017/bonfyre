package VaultScan;
# VaultScan — Find notes by type, status, path, and frontmatter criteria.
# Uses VaultParse for reading. Returns filtered lists of parsed notes.

use strict;
use warnings;
use File::Find;
use File::Spec;
use Exporter 'import';

use lib __FILE__ =~ s|/[^/]+$||r;
use VaultParse qw(parse_note);

our @EXPORT_OK = qw(
    scan_vault
    find_notes
    find_notes_by_profile
);

# ──────────────────────────────────────────────
# scan_vault($vault_root, @subdirs) → arrayref of parsed notes
#   Recursively finds all .md files under given subdirs.
#   Skips dotfiles/folders, 10-Code/, Templates/.
# ──────────────────────────────────────────────
sub scan_vault {
    my ($vault_root, @subdirs) = @_;

    @subdirs = ('.') unless @subdirs;

    my @files;
    my @search_dirs = map { File::Spec->catdir($vault_root, $_) } @subdirs;

    # Only search dirs that exist
    @search_dirs = grep { -d $_ } @search_dirs;
    return [] unless @search_dirs;

    find({
        wanted => sub {
            return unless -f && /\.md$/;
            my $rel = File::Spec->abs2rel($_, $vault_root);
            # Skip internals
            return if $rel =~ m{^\.};
            return if $rel =~ m{^10-Code/};
            return if $rel =~ m{^Templates/};
            push @files, $File::Find::name;
        },
        no_chdir => 1,
    }, @search_dirs);

    my @notes;
    for my $f (sort @files) {
        eval {
            my $note = parse_note($f);
            push @notes, $note if $note && $note->{frontmatter}{type};
        };
        if ($@) {
            warn "VaultScan: skipping $f: $@";
        }
    }

    return \@notes;
}

# ──────────────────────────────────────────────
# find_notes($vault_root, $selector) → arrayref of parsed notes
#   $selector is a hashref with optional keys:
#     type     => string or arrayref
#     status   => string or arrayref
#     verdict  => string or arrayref
#     stage    => string or arrayref
#     project_created => string or arrayref (supports null → missing key)
#     paths    => arrayref of relative subdirs to search
#     priority => string or arrayref
# ──────────────────────────────────────────────
sub find_notes {
    my ($vault_root, $selector) = @_;
    $selector ||= {};

    my @paths = @{ $selector->{paths} || ['.'] };
    my $all_notes = scan_vault($vault_root, @paths);

    my @matched;
    for my $note (@$all_notes) {
        next unless _matches($note, $selector);
        push @matched, $note;
    }

    return \@matched;
}

# ──────────────────────────────────────────────
# find_notes_by_profile($vault_root, $profile) → arrayref
#   Convenience: extracts selector from a loaded profile hashref.
# ──────────────────────────────────────────────
sub find_notes_by_profile {
    my ($vault_root, $profile) = @_;
    return find_notes($vault_root, $profile->{selector});
}

# ── Internal matching ──

sub _matches {
    my ($note, $sel) = @_;
    my $fm = $note->{frontmatter};

    # Skip special digest selector
    return 0 if $sel->{_special};

    for my $key (qw(type status verdict stage priority)) {
        next unless exists $sel->{$key};
        my $allowed = _as_array($sel->{$key});
        my $val = $fm->{$key} // '';
        return 0 unless grep { lc($val) eq lc($_) } @$allowed;
    }

    # project_created: support null meaning "key not present"
    if (exists $sel->{project_created}) {
        my $allowed = _as_array($sel->{project_created});
        my $val = $fm->{project_created};
        my $match = 0;
        for my $a (@$allowed) {
            if (!defined $a || $a eq 'null') {
                $match = 1 if !defined $val || $val eq '';
            } else {
                $match = 1 if defined $val && lc($val) eq lc($a);
            }
        }
        return 0 unless $match;
    }

    return 1;
}

sub _as_array {
    my ($val) = @_;
    return $val if ref $val eq 'ARRAY';
    return [$val];
}

1;
