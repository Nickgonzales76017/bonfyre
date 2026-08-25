package VaultParse;
# VaultParse — Parse Obsidian markdown files: frontmatter, sections, wiki links.
# No external dependencies beyond core Perl + JSON::PP.

use strict;
use warnings;
use JSON::PP;
use File::Basename;
use Exporter 'import';

our @EXPORT_OK = qw(
    parse_note
    parse_frontmatter
    extract_sections
    extract_wiki_links
    section_text
);

# ──────────────────────────────────────────────
# parse_note($filepath) → hashref
#   Returns: { path, filename, frontmatter, body, sections, wiki_links }
# ──────────────────────────────────────────────
sub parse_note {
    my ($filepath) = @_;
    die "parse_note: file not found: $filepath\n" unless -f $filepath;

    open my $fh, '<:encoding(UTF-8)', $filepath
        or die "Cannot open $filepath: $!\n";
    my $raw = do { local $/; <$fh> };
    close $fh;

    my $fm   = parse_frontmatter(\$raw);
    my $body = _strip_frontmatter($raw);
    my $secs = extract_sections($body);
    my $links = extract_wiki_links($body);

    return {
        path        => $filepath,
        filename    => basename($filepath, '.md'),
        frontmatter => $fm,
        body        => $body,
        sections    => $secs,
        wiki_links  => $links,
    };
}

# ──────────────────────────────────────────────
# parse_frontmatter(\$raw_content) → hashref
#   Extracts YAML frontmatter between --- delimiters.
#   Handles: scalars, arrays (both [] and - styles), quoted strings.
# ──────────────────────────────────────────────
sub parse_frontmatter {
    my ($raw_ref) = @_;
    my $raw = ref $raw_ref ? $$raw_ref : $raw_ref;

    return {} unless $raw =~ /\A---\s*\n(.*?)\n---/s;
    my $yaml_block = $1;

    my %fm;
    my $current_key;

    for my $line (split /\n/, $yaml_block) {
        # Array continuation: "  - value"
        if ($line =~ /^\s+-\s+(.+)/ && $current_key) {
            my $val = _clean_yaml_value($1);
            $fm{$current_key} = [] unless ref $fm{$current_key} eq 'ARRAY';
            push @{ $fm{$current_key} }, $val;
            next;
        }

        # Key: value pair
        if ($line =~ /^(\w[\w\-]*)\s*:\s*(.*)/) {
            my ($key, $val) = ($1, $2);
            $current_key = $key;

            if ($val =~ /^\[/) {
                # Inline array: [a, b, c]
                $val =~ s/^\[//;
                $val =~ s/\]$//;
                my @items = map { _clean_yaml_value($_) } split /\s*,\s*/, $val;
                $fm{$key} = \@items;
            } elsif ($val =~ /\S/) {
                $fm{$key} = _clean_yaml_value($val);
            } else {
                # Value will come as array items below
                $fm{$key} = [];
            }
        }
    }

    return \%fm;
}

# ──────────────────────────────────────────────
# extract_sections($body) → arrayref of hashrefs
#   Returns: [ { level, heading, text, raw }, ... ]
#   Splits on ## headings. Level 1 = #, Level 2 = ##, etc.
# ──────────────────────────────────────────────
sub extract_sections {
    my ($body) = @_;
    my @sections;
    my @lines = split /\n/, $body;
    my $current;

    for my $line (@lines) {
        if ($line =~ /^(#{1,6})\s+(.+)/) {
            push @sections, $current if $current;
            $current = {
                level   => length($1),
                heading => $2,
                lines   => [],
            };
        } elsif ($current) {
            push @{ $current->{lines} }, $line;
        }
    }
    push @sections, $current if $current;

    for my $sec (@sections) {
        $sec->{text} = join("\n", @{ $sec->{lines} });
        $sec->{text} =~ s/\A\n+//;
        $sec->{text} =~ s/\n+\z//;
        delete $sec->{lines};
    }

    return \@sections;
}

# ──────────────────────────────────────────────
# extract_wiki_links($body) → arrayref of strings
#   Returns unique [[target]] values, without display aliases.
# ──────────────────────────────────────────────
sub extract_wiki_links {
    my ($body) = @_;
    my %seen;
    my @links;

    while ($body =~ /\[\[([^\]|]+)(?:\|[^\]]+)?\]\]/g) {
        my $target = $1;
        $target =~ s/^\s+//;
        $target =~ s/\s+$//;
        unless ($seen{$target}++) {
            push @links, $target;
        }
    }

    return \@links;
}

# ──────────────────────────────────────────────
# section_text($note, @heading_patterns) → string
#   Find sections whose heading matches any pattern (case-insensitive)
#   and concatenate their text.
# ──────────────────────────────────────────────
sub section_text {
    my ($note, @patterns) = @_;
    my @matched;

    for my $sec (@{ $note->{sections} || [] }) {
        for my $pat (@patterns) {
            if ($sec->{heading} =~ /$pat/i) {
                push @matched, "## $sec->{heading}\n$sec->{text}";
                last;
            }
        }
    }

    return join("\n\n", @matched);
}

# ── Internal helpers ──

sub _strip_frontmatter {
    my ($raw) = @_;
    $raw =~ s/\A---\s*\n.*?\n---\s*\n?//s;
    return $raw;
}

sub _clean_yaml_value {
    my ($v) = @_;
    $v =~ s/^\s+//;
    $v =~ s/\s+$//;
    $v =~ s/^["']//;
    $v =~ s/["']$//;
    return $v;
}

1;
