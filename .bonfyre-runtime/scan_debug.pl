#!/usr/bin/env perl
use strict;
use warnings;
use utf8;
use FindBin qw($Bin);
use lib "$Bin/../10-Code/NightlyBrainstorm/lib";
use File::Spec;
use JSON::PP;

use VaultScan qw(scan_vault);

my $vault_root = File::Spec->rel2abs(File::Spec->catdir($Bin, '..', '..'));
print "Computed vault_root: $vault_root\n\n";

my @subdirs = ('01-Ideas','02-Projects','03-Research','04-Systems','05-Monetization','06-Logs','07-Agent Briefs');
my $notes = scan_vault($vault_root, @subdirs);

print "Total parsed notes with frontmatter: " . scalar(@$notes) . "\n\n";

my $count = 0;
for my $note (@$notes) {
    last if $count++ >= 50;
    my $rel = File::Spec->abs2rel($note->{path}, $vault_root);
    my $fm = $note->{frontmatter} || {};
    my $keys = join(',', sort keys %$fm);
    my $type = $fm->{type} // '';
    my $status = $fm->{status} // '';
    print sprintf("%3d. %s\n    type: %s | status: %s | fm keys: %s\n", $count, $rel, $type, $status, $keys);
}

if ($count == 0) {
    print "No parsed notes found in those subdirs.\n";
}
