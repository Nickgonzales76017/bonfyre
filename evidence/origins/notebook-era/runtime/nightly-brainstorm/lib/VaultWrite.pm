package VaultWrite;
# VaultWrite — Write AI outputs back into the vault.
# Two modes: append (add section to existing note) and create (new file).
# Never overwrites existing content. Append-only by design.

use strict;
use warnings;
use utf8;
use File::Path qw(make_path);
use File::Basename;
use POSIX qw(strftime);
use Exporter 'import';

our @EXPORT_OK = qw(
    write_output
    append_to_note
    create_note
);

# ──────────────────────────────────────────────
# write_output($profile, $note, $ai_output, $vault_root) → $path_written
#   Routes to append or create based on profile output config.
# ──────────────────────────────────────────────
sub write_output {
    my ($profile, $note, $ai_output, $vault_root) = @_;
    my $out = $profile->{output};
    my $date = strftime('%Y-%m-%d', localtime);

    if ($out->{mode} eq 'append') {
        my $heading = $out->{heading};
        $heading =~ s/\{\{date\}\}/$date/g;

        my $target;
        if ($out->{destination} eq 'source') {
            $target = $note->{path};
        } else {
            $target = File::Spec->catfile($vault_root, $out->{destination});
        }

        return append_to_note($target, $heading, $ai_output);

    } elsif ($out->{mode} eq 'create') {
        my $title = $note->{frontmatter}{title} // $note->{filename};
        my $path_tpl = $out->{path_template};
        $path_tpl =~ s/\{\{title\}\}/$title/g;
        $path_tpl =~ s/\{\{date\}\}/$date/g;

        my $full_path = File::Spec->catfile($vault_root, $path_tpl);

        my $fm_data = $out->{frontmatter} || {};
        my %fm;
        for my $k (keys %$fm_data) {
            my $v = $fm_data->{$k};
            if (ref $v eq 'ARRAY') {
                $fm{$k} = $v;
            } else {
                $v =~ s/\{\{date\}\}/$date/g;
                $v =~ s/\{\{source_path\}\}/$note->{path}/g;
                $v =~ s/\{\{title\}\}/$title/g;
                $fm{$k} = $v;
            }
        }

        return create_note($full_path, \%fm, $ai_output);
    }

    die "Unknown output mode: $out->{mode}\n";
}

# ──────────────────────────────────────────────
# append_to_note($filepath, $heading, $content) → $filepath
#   Appends a new section to an existing note.
#   Checks for duplicate heading to prevent double-runs.
# ──────────────────────────────────────────────
sub append_to_note {
    my ($filepath, $heading, $content) = @_;
    die "append_to_note: file not found: $filepath\n" unless -f $filepath;

    # Read existing content to check for duplicates
    open my $rfh, '<:encoding(UTF-8)', $filepath
        or die "Cannot read $filepath: $!\n";
    my $existing = do { local $/; <$rfh> };
    close $rfh;

    # Prevent duplicate sections from same day
    if (index($existing, $heading) >= 0) {
        warn "VaultWrite: skipping duplicate section '$heading' in $filepath\n";
        return $filepath;
    }

    # Append
    open my $wfh, '>>:encoding(UTF-8)', $filepath
        or die "Cannot append to $filepath: $!\n";
    print $wfh "\n\n$heading\n\n$content\n";
    close $wfh;

    return $filepath;
}

# ──────────────────────────────────────────────
# create_note($filepath, $frontmatter_hash, $body) → $filepath
#   Creates a new note file. Will NOT overwrite existing files.
# ──────────────────────────────────────────────
sub create_note {
    my ($filepath, $fm, $body) = @_;

    if (-f $filepath) {
        warn "VaultWrite: refusing to overwrite existing file: $filepath\n";
        return $filepath;
    }

    # Ensure directory exists
    my $dir = dirname($filepath);
    make_path($dir) unless -d $dir;

    # Build frontmatter
    my $fm_text = "---\n";
    for my $key (sort keys %$fm) {
        my $val = $fm->{$key};
        if (ref $val eq 'ARRAY') {
            $fm_text .= "$key:\n";
            for my $item (@$val) {
                $fm_text .= "  - $item\n";
            }
        } else {
            # Quote values with special chars
            if ($val =~ /[:#\[\]{}|>&*!]/ || $val =~ /^\s/ || $val =~ /\s$/) {
                $fm_text .= "$key: \"$val\"\n";
            } else {
                $fm_text .= "$key: $val\n";
            }
        }
    }
    $fm_text .= "---\n\n";

    open my $fh, '>:encoding(UTF-8)', $filepath
        or die "Cannot create $filepath: $!\n";
    print $fh $fm_text;
    print $fh $body;
    print $fh "\n";
    close $fh;

    return $filepath;
}

1;
