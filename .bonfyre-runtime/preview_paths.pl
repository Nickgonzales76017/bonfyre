#!/usr/bin/env perl
use strict;
use warnings;
use FindBin qw($Bin);
use lib "$Bin/../10-Code/NightlyBrainstorm/lib";
use File::Spec;
use POSIX qw(strftime);
use JSON::PP;
use Getopt::Long;
use open qw(:std :encoding(UTF-8));
binmode STDOUT, ':encoding(UTF-8)';

use VaultScan qw(find_notes_by_profile);

my $root;
my $pass;
my $limit = 5;
GetOptions(
    'root=s' => \$root,
    'pass=s' => \$pass,
    'limit=i' => \$limit,
) or die "Usage: preview_paths.pl --root /path/to/vault --pass NAME [--limit N]\n";
$root //= File::Spec->rel2abs(".");
die "--pass required\n" unless $pass;

my $prof_fn = File::Spec->catfile($root, '10-Code', 'NightlyBrainstorm', 'profiles', "$pass.json");
open my $fh, '<:encoding(UTF-8)', $prof_fn or die "No profile $prof_fn: $!\n";
my $raw = do { local $/; <$fh> };
close $fh;
my $json = JSON::PP->new->utf8->relaxed->decode($raw);

# Load notes via VaultScan
my $notes = find_notes_by_profile($root, $json);
print "Found " . scalar(@$notes) . " candidate notes for pass '$pass'\n";

my $date = strftime('%Y-%m-%d', localtime);
my $count = 0;
for my $note (@$notes) {
    last if $count++ >= $limit;
    my $title = $note->{frontmatter}{title} // $note->{filename};
    print "\nNote: $note->{path}\n";
    my $out = $json->{output} || {};

    # Text output
    if (($out->{mode} || '') eq 'append') {
        my $heading = $out->{heading} || 'AI Output';
        my $target;
        if ($out->{destination} && $out->{destination} eq 'source') {
            $target = $note->{path};
        } else {
            $target = File::Spec->catfile($root, $out->{destination} || '(no-destination)');
        }
        print "  Text append target: $target\n";
        print "  Append heading: $heading\n";
    } elsif (($out->{mode} || '') eq 'create') {
        my $tpl = $out->{path_template} || '(no path_template)';
        my $p = $tpl;
        $p =~ s/\{\{title\}\}/$title/g;
        $p =~ s/\{\{date\}\}/$date/g;
        print "  Create path: " . File::Spec->catfile($root, $p) . "\n";

        # also show frontmatter target if present
        if ($out->{frontmatter} && $out->{frontmatter}{audio}) {
            my $audio = $out->{frontmatter}{audio};
            $audio =~ s/\{\{date\}\}/$date/g;
            print "  Frontmatter audio link: $audio\n";
        }
    } else {
        print "  No text output (mode: " . ($out->{mode}||'(none)') . ")\n";
    }

    # Audio output via audio block or audio config
    if ($out->{audio} && $out->{audio}{enabled}) {
        my $cat = $out->{audio}{category} || 'Daily';
        my $fn_tpl = $out->{audio}{filename_template} || "${title}-${date}.wav";
        $fn_tpl =~ s/\{\{title\}\}/$title/g;
        $fn_tpl =~ s/\{\{date\}\}/$date/g;
        my $audio_path = File::Spec->catfile($root, '07-Audio', $cat, $fn_tpl);
        print "  Audio path: $audio_path\n";
    } elsif ($json->{output}{frontmatter} && $json->{output}{frontmatter}{audio}) {
        my $audio = $json->{output}{frontmatter}{audio};
        $audio =~ s/\{\{date\}\}/$date/g;
        print "  Audio frontmatter link: $audio\n";
    } elsif ($json->{output}{audio} && $json->{output}{audio}{enabled}) {
        my $cat = $json->{output}{audio}{category} || 'Daily';
        my $fn_tpl = $json->{output}{audio}{filename_template} || "${title}-${date}.wav";
        $fn_tpl =~ s/\{\{title\}\}/$title/g;
        $fn_tpl =~ s/\{\{date\}\}/$date/g;
        my $audio_path = File::Spec->catfile($root, '07-Audio', $cat, $fn_tpl);
        print "  Audio path: $audio_path\n";
    }
}

exit 0;
