package ContextBundle;
# ContextBundle — Build bounded context bundles per job profile.
# Each profile defines a context recipe. This module enforces it.
# No unbounded retrieval. No recursive expansion. Hardcoded recipes.

use strict;
use warnings;
use File::Spec;
use Exporter 'import';

use lib __FILE__ =~ s|/[^/]+$||r;
use VaultParse qw(parse_note section_text);

our @EXPORT_OK = qw(
    build_context
    build_digest_context
);

# ──────────────────────────────────────────────
# build_context($note, $profile, $vault_root, $all_notes) → hashref
#   Returns: { frontmatter_yaml, body_text, related_notes_text }
#   Bounded by the profile's context_recipe.
# ──────────────────────────────────────────────
sub build_context {
    my ($note, $profile, $vault_root, $all_notes) = @_;
    my $recipe = $profile->{context_recipe};

    # 1. Format frontmatter as YAML-like text
    my $fm_text = _format_frontmatter($note->{frontmatter});

    # 2. Extract only allowed sections from body
    my $body_text;
    if ($recipe->{include_sections}) {
        $body_text = section_text($note, @{ $recipe->{include_sections} });
        # If no sections matched, fall back to full body (trimmed)
        $body_text = $note->{body} unless $body_text =~ /\S/;
    } else {
        $body_text = $note->{body};
    }

    # 3. Build related notes context
    my $related_text = _build_related($note, $recipe, $vault_root, $all_notes);

    # 4. Enforce max context size
    my $max_chars = $recipe->{max_context_chars} || 8000;
    $body_text    = _truncate($body_text, int($max_chars * 0.5));
    $related_text = _truncate($related_text, int($max_chars * 0.3));

    return {
        frontmatter_yaml   => $fm_text,
        body_text          => $body_text,
        related_notes_text => $related_text,
    };
}

# ──────────────────────────────────────────────
# build_digest_context($run_log) → string
#   Takes the structured run log from the orchestrator
#   and formats it for the morning digest prompt.
# ──────────────────────────────────────────────
sub build_digest_context {
    my ($run_log) = @_;
    my @entries;

    for my $entry (@$run_log) {
        my $type    = $entry->{type}    // 'unknown';
        my $title   = $entry->{title}   // 'untitled';
        my $pass    = $entry->{pass}    // 'unknown';
        my $output  = $entry->{output}  // '';

        # Trim output to keep digest context manageable
        $output = _truncate($output, 800);

        push @entries, "### [$pass] $title (type: $type)\n$output";
    }

    return join("\n\n---\n\n", @entries);
}

# ── Internal: build related notes text ──

sub _build_related {
    my ($note, $recipe, $vault_root, $all_notes) = @_;
    my @parts;

    # Follow explicit link fields from frontmatter (idea_link, source_project, etc.)
    if ($recipe->{follow_links}) {
        for my $field (keys %{ $recipe->{follow_links} }) {
            my $spec = $recipe->{follow_links}{$field};
            my $link_val = $note->{frontmatter}{$field};
            next unless $link_val;

            my $linked = _resolve_link($link_val, $vault_root, $all_notes);
            if ($linked) {
                my $text = _extract_at_depth($linked, $spec->{depth} || 'frontmatter+summary');
                push @parts, "--- Linked: $field ---\n$text" if $text =~ /\S/;
            }
        }
    }

    # Related by type (ideas, systems, concepts, pipelines)
    my $fm = $note->{frontmatter};
    my $wiki_links = $note->{wiki_links} || [];

    # Related ideas
    if (my $max = $recipe->{max_related_ideas}) {
        my @related = _find_related_by_type($all_notes, 'idea', $wiki_links, $max, $note->{path});
        for my $r (@related) {
            my $text = _extract_at_depth($r, $recipe->{related_depth} || 'frontmatter+summary');
            my $label = $r->{frontmatter}{title} // $r->{filename} // 'untitled';
            push @parts, "--- Related idea: $label ---\n$text" if $text =~ /\S/;
        }
    }

    # Related concepts
    if (my $max = $recipe->{max_related_concepts}) {
        my @related = _find_related_by_type($all_notes, 'concept', $wiki_links, $max, $note->{path});
        for my $r (@related) {
            my $text = _extract_at_depth($r, $recipe->{related_depth} || 'frontmatter+summary');
            my $label = $r->{frontmatter}{title} // $r->{filename} // 'untitled';
            push @parts, "--- Related concept: $label ---\n$text" if $text =~ /\S/;
        }
    }

    # Related systems
    if (my $max = $recipe->{max_related_systems}) {
        my @related = _find_related_by_type($all_notes, 'system', $wiki_links, $max, $note->{path});
        for my $r (@related) {
            my $text = _extract_at_depth($r, $recipe->{related_depth} || 'frontmatter+summary');
            my $label = $r->{frontmatter}{title} // $r->{filename} // 'untitled';
            push @parts, "--- Related system: $label ---\n$text" if $text =~ /\S/;
        }
    }

    # Related pipelines
    if (my $max = $recipe->{max_related_pipelines}) {
        my @related = _find_related_by_type($all_notes, 'pipeline', $wiki_links, $max, $note->{path});
        for my $r (@related) {
            my $text = _extract_at_depth($r, $recipe->{related_depth} || 'frontmatter+summary');
            my $label = $r->{frontmatter}{title} // $r->{filename} // 'untitled';
            push @parts, "--- Related pipeline: $label ---\n$text" if $text =~ /\S/;
        }
    }

    return join("\n\n", @parts);
}

# Resolve a [[wiki link]] or frontmatter link to a parsed note
sub _resolve_link {
    my ($link_val, $vault_root, $all_notes) = @_;

    # Strip [[ ]] if present
    $link_val =~ s/^\[\[//;
    $link_val =~ s/\]\]$//;
    $link_val =~ s/\|.*//;  # Strip alias

    for my $n (@$all_notes) {
        my $rel = File::Spec->abs2rel($n->{path}, $vault_root);
        $rel =~ s/\.md$//;
        return $n if $rel eq $link_val;
        return $n if $n->{filename} eq $link_val;
        # Match on title
        return $n if ($n->{frontmatter}{title} // '') eq $link_val;
    }

    return undef;
}

# Find related notes by type that appear in wiki_links
sub _find_related_by_type {
    my ($all_notes, $target_type, $wiki_links, $max, $exclude_path) = @_;

    # Build lookup of wiki link targets
    my %link_targets = map { $_ => 1 } @$wiki_links;

    my @candidates;
    for my $n (@$all_notes) {
        next if $n->{path} eq $exclude_path;
        next unless lc($n->{frontmatter}{type} // '') eq lc($target_type);

        # Prioritize notes that are explicitly linked
        my $linked = 0;
        for my $lt (keys %link_targets) {
            if ($n->{filename} =~ /\Q$lt\E/i ||
                ($n->{frontmatter}{title} // '') =~ /\Q$lt\E/i ||
                $n->{path} =~ /\Q$lt\E/i) {
                $linked = 1;
                last;
            }
        }

        push @candidates, { note => $n, linked => $linked };
    }

    # Sort: linked first, then alphabetical
    @candidates = sort {
        $b->{linked} <=> $a->{linked}
        || ($a->{note}{frontmatter}{title} // '') cmp ($b->{note}{frontmatter}{title} // '')
    } @candidates;

    my @result = map { $_->{note} } splice(@candidates, 0, $max);
    return @result;
}

# Extract note content at a specified depth level
sub _extract_at_depth {
    my ($note, $depth) = @_;
    my @parts;

    if ($depth =~ /frontmatter/) {
        push @parts, _format_frontmatter($note->{frontmatter});
    }

    if ($depth =~ /summary/) {
        my $sum = section_text($note, 'Summary', 'Objective', 'Purpose');
        push @parts, $sum if $sum =~ /\S/;
    }

    if ($depth =~ /core_mechanism/) {
        my $cm = section_text($note, 'Core Mechanism', 'Core Insight');
        push @parts, $cm if $cm =~ /\S/;
    }

    if ($depth =~ /purpose/) {
        my $p = section_text($note, 'Purpose');
        push @parts, $p if $p =~ /\S/;
    }

    if ($depth =~ /inputs/) {
        my $i = section_text($note, 'Inputs');
        push @parts, $i if $i =~ /\S/;
    }

    if ($depth =~ /outputs/) {
        my $o = section_text($note, 'Outputs');
        push @parts, $o if $o =~ /\S/;
    }

    return join("\n\n", @parts);
}

# Format frontmatter hash as readable YAML-like text
sub _format_frontmatter {
    my ($fm) = @_;
    my @lines;

    for my $key (sort keys %$fm) {
        my $val = $fm->{$key};
        if (ref $val eq 'ARRAY') {
            push @lines, "$key: [" . join(', ', @$val) . "]";
        } else {
            push @lines, "$key: $val";
        }
    }

    return join("\n", @lines);
}

# Sentence-boundary-aware truncation
sub _truncate {
    my ($text, $max) = @_;
    return $text unless defined $text && length($text) > $max;

    # Cut at the last sentence boundary (. ! ?) before the limit
    my $chunk = substr($text, 0, $max);
    if ($chunk =~ /^(.+[.!?])\s/s) {
        return $1 . "\n[...truncated...]";
    }
    # Fallback: cut at last whitespace to avoid mid-word break
    if ($chunk =~ /^(.+)\s/s) {
        return $1 . "\n[...truncated...]";
    }
    return $chunk . "\n[...truncated...]";
}

1;
