package PiperTTS;
# PiperTTS — Text-to-speech via Piper. Converts text to .wav files.
# Handles: text cleanup for spoken delivery, Piper subprocess, output routing.

use strict;
use warnings;
use File::Spec;
use File::Path qw(make_path);
use File::Basename;
use File::Temp qw(tempfile);
use POSIX qw(strftime);
use Exporter 'import';

our @EXPORT_OK = qw(
    synthesize
    clean_for_speech
    synthesize_note_audio
);

# ──────────────────────────────────────────────
# synthesize($text, $output_path, $config) → $output_path
#   Converts text to a .wav file via Piper.
#   $config hashref: { piper_bin, piper_model, sentence_silence, volume }
# ──────────────────────────────────────────────
sub synthesize {
    my ($text, $output_path, $config) = @_;

    my $piper_bin   = $config->{piper_bin}   || 'piper';
    my $model       = $config->{piper_model} or die "PiperTTS: piper_model required\n";

    die "PiperTTS: model not found: $model\n" unless -f $model;

    # Clean text for spoken delivery
    my $clean = clean_for_speech($text);

    return undef unless $clean =~ /\S/;

    # Ensure output directory exists
    my $dir = dirname($output_path);
    make_path($dir) unless -d $dir;

    # Write text to temp file (avoids pipe/encoding issues)
    my ($tmp_fh, $tmp_path) = tempfile(SUFFIX => '.txt', UNLINK => 1);
    binmode($tmp_fh, ':encoding(UTF-8)');
    print $tmp_fh $clean;
    close $tmp_fh;

    # Build command
    my @cmd = ($piper_bin, '-m', $model, '-f', $output_path);

    # Optional: sentence silence gap
    if ($config->{sentence_silence}) {
        push @cmd, '--sentence-silence', $config->{sentence_silence};
    }

    # Optional: volume
    if ($config->{volume}) {
        push @cmd, '--volume', $config->{volume};
    }

    my $cmd_str = join(' ', map { _shell_escape($_) } @cmd)
                . ' < ' . _shell_escape($tmp_path) . ' 2>/dev/null';

    system($cmd_str);
    my $exit = $? >> 8;

    if ($exit != 0) {
        warn "PiperTTS: piper failed (exit $exit) for $output_path\n";
        return undef;
    }

    unless (-f $output_path && -s $output_path > 0) {
        warn "PiperTTS: output file empty or missing: $output_path\n";
        return undef;
    }

    return $output_path;
}

# ──────────────────────────────────────────────
# clean_for_speech($text) → $cleaned
#   Strip markdown, symbols, formatting for natural spoken delivery.
# ──────────────────────────────────────────────
sub clean_for_speech {
    my ($text) = @_;

    # Remove YAML frontmatter
    $text =~ s/\A---\s*\n.*?\n---\s*\n?//s;

    # Remove markdown headings markers (keep text)
    $text =~ s/^#{1,6}\s+//gm;

    # Remove wiki links, keep display text or target
    $text =~ s/\[\[([^\]|]+)\|([^\]]+)\]\]/$2/g;  # [[target|display]] → display
    $text =~ s/\[\[([^\]]+)\]\]/$1/g;               # [[target]] → target

    # Remove markdown links, keep text
    $text =~ s/\[([^\]]+)\]\([^)]+\)/$1/g;

    # Remove bold/italic markers
    $text =~ s/\*{1,3}//g;
    $text =~ s/_{1,3}//g;

    # Remove code blocks
    $text =~ s/```.*?```//gs;
    $text =~ s/`([^`]+)`/$1/g;

    # Remove bullet markers
    $text =~ s/^\s*[-*+]\s+//gm;

    # Remove numbered list markers
    $text =~ s/^\s*\d+\.\s+//gm;

    # Remove checkbox markers
    $text =~ s/^\s*\[[ xX]\]\s*//gm;

    # Remove horizontal rules
    $text =~ s/^---+\s*$//gm;

    # Remove table formatting
    $text =~ s/\|//g;
    $text =~ s/^[-:]+$//gm;

    # Remove blockquote markers
    $text =~ s/^>\s*//gm;

    # Remove HTML tags
    $text =~ s/<[^>]+>//g;

    # Remove emoji/unicode symbols common in notes
    $text =~ s/[👉🔥🟢🟡🔵🟣🟠🔴⚡⚙️🧠🧱🚀✓✗▸●◆→←↓↑═]//g;

    # Clean up multiple blank lines
    $text =~ s/\n{3,}/\n\n/g;

    # Clean up leading/trailing whitespace
    $text =~ s/^\s+//;
    $text =~ s/\s+$//;

    return $text;
}

# ──────────────────────────────────────────────
# synthesize_note_audio($text, $category, $title, $vault_root, $config) → $path
#   High-level: write audio for a note into the vault's 07-Audio/ tree.
#   $category: 'Daily', 'Projects', 'Ideas', 'Distribution'
# ──────────────────────────────────────────────
sub synthesize_note_audio {
    my ($text, $category, $title, $vault_root, $config) = @_;
    my $date = strftime('%Y-%m-%d', localtime);

    # Sanitize title for filename
    my $safe_title = $title;
    $safe_title =~ s/[^\w\s-]//g;
    $safe_title =~ s/\s+/-/g;

    my $filename = "$safe_title - $date.wav";
    my $output_path = File::Spec->catfile($vault_root, '07-Audio', $category, $filename);

    return synthesize($text, $output_path, $config);
}

# ── Internal ──

sub _shell_escape {
    my ($arg) = @_;
    $arg =~ s/'/'\\''/g;
    return "'$arg'";
}

1;
