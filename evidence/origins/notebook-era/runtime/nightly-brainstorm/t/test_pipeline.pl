#!/usr/bin/env perl
# ═══════════════════════════════════════════════════════════════
# t/test_pipeline.pl — Integration tests for the nightly cognition system
# ═══════════════════════════════════════════════════════════════
# Run: perl t/test_pipeline.pl
# Tests vault parsing, scanning, context building, prompt assembly,
# and write operations using the real vault data.
# ═══════════════════════════════════════════════════════════════

use strict;
use warnings;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use File::Spec;
use File::Temp qw(tempdir tempfile);
use File::Path qw(remove_tree);
use JSON::PP;

use VaultParse    qw(parse_note parse_frontmatter extract_sections extract_wiki_links section_text);
use VaultScan     qw(scan_vault find_notes);
use ContextBundle qw(build_context build_digest_context);
use LlamaRun      qw(build_prompt load_config);
use VaultWrite    qw(append_to_note create_note);
use PiperTTS      qw(clean_for_speech);
use NightlyQueue  qw(load_queue save_queue enqueue_pass next_queued_item mark_queue_item queue_status queue_path);

my $JSON  = JSON::PP->new->utf8->pretty;
my $vault = File::Spec->catdir($Bin, '..', '..', '..');
my $tests = 0;
my $pass  = 0;
my $fail  = 0;

# ═══════════════════════════════════════════════
# Test Helpers
# ═══════════════════════════════════════════════

sub ok {
    my ($cond, $name) = @_;
    $tests++;
    if ($cond) {
        print "  ✓ $name\n";
        $pass++;
    } else {
        print "  ✗ FAIL: $name\n";
        $fail++;
    }
}

sub section {
    my ($name) = @_;
    print "\n── $name ──\n";
}

# ═══════════════════════════════════════════════
# Test: VaultParse
# ═══════════════════════════════════════════════
section("VaultParse");

# Parse a real idea note
my $idea_path = File::Spec->catfile($vault, '01-Ideas', 'Local AI Transcription Service.md');
if (-f $idea_path) {
    my $note = parse_note($idea_path);
    ok($note->{frontmatter}{type} eq 'idea', 'parse idea type');
    ok($note->{frontmatter}{title} =~ /transcription/i, 'parse idea title');
    ok(ref $note->{sections} eq 'ARRAY', 'sections is arrayref');
    ok(scalar @{$note->{sections}} > 0, 'has sections');
    ok(ref $note->{wiki_links} eq 'ARRAY', 'wiki_links is arrayref');

    my $sum = section_text($note, 'Summary');
    ok(length($sum) > 0, 'section_text finds Summary');
} else {
    print "  (skipping idea parse — file not found)\n";
}

# Parse a real project note
my $proj_path = File::Spec->catfile($vault, '02-Projects', 'Project - Local AI Transcription Service.md');
if (-f $proj_path) {
    my $note = parse_note($proj_path);
    ok($note->{frontmatter}{type} eq 'project', 'parse project type');
    ok($note->{frontmatter}{status}, 'project has status');
    ok($note->{frontmatter}{priority}, 'project has priority');
    ok($note->{frontmatter}{idea_link}, 'project has idea_link');
} else {
    print "  (skipping project parse — file not found)\n";
}

# Frontmatter edge cases
my $fm_test = parse_frontmatter(\"---\ntype: idea\ntags: [foo, bar, baz]\nstatus: active\n---\nbody here");
ok($fm_test->{type} eq 'idea', 'parse inline scalar');
ok(ref $fm_test->{tags} eq 'ARRAY', 'parse inline array');
ok(scalar @{$fm_test->{tags}} == 3, 'inline array has 3 items');

my $fm_test2 = parse_frontmatter(\"---\ntype: project\ntags:\n  - alpha\n  - beta\n---\n");
ok(ref $fm_test2->{tags} eq 'ARRAY', 'parse dash-style array');
ok(scalar @{$fm_test2->{tags}} == 2, 'dash array has 2 items');

# Wiki link extraction
my $body = "Check [[01-Ideas/Foo]] and [[Bar|display name]] plus [[Baz]]";
my $links = extract_wiki_links($body);
ok(scalar @$links == 3, 'extracts 3 wiki links');
ok($links->[0] eq '01-Ideas/Foo', 'full-path link');
ok($links->[1] eq 'Bar', 'alias link strips display');
ok($links->[2] eq 'Baz', 'simple link');

# ═══════════════════════════════════════════════
# Test: VaultScan
# ═══════════════════════════════════════════════
section("VaultScan");

my $all = scan_vault($vault, '01-Ideas');
ok(ref $all eq 'ARRAY', 'scan returns arrayref');
ok(scalar @$all > 0, 'scan found ideas');

my $ideas = find_notes($vault, { type => 'idea', paths => ['01-Ideas/'] });
ok(scalar @$ideas > 0, 'find_notes returns ideas');
ok($ideas->[0]{frontmatter}{type} eq 'idea', 'all filtered notes are ideas');

my $active_ideas = find_notes($vault, {
    type => 'idea', status => ['active', 'exploratory'], paths => ['01-Ideas/']
});
ok(scalar @$active_ideas <= scalar @$ideas, 'active filter reduces count');

my $projects = find_notes($vault, { type => 'project', paths => ['02-Projects/'] });
ok(scalar @$projects > 0, 'find_notes returns projects');

# ═══════════════════════════════════════════════
# Test: ContextBundle
# ═══════════════════════════════════════════════
section("ContextBundle");

if (@$ideas > 0) {
    # Load idea_expand profile
    my $profile_path = File::Spec->catfile($Bin, '..', 'profiles', 'idea_expand.json');
    my $profile;
    if (-f $profile_path) {
        open my $fh, '<', $profile_path or die $!;
        $profile = $JSON->decode(do { local $/; <$fh> });
        close $fh;
    }

    if ($profile) {
        my $all_notes = scan_vault($vault,
            '01-Ideas', '02-Projects', '03-Research',
            '04-Systems', '05-Monetization'
        );

        my $ctx = build_context($ideas->[0], $profile, $vault, $all_notes);
        ok(defined $ctx->{frontmatter_yaml}, 'context has frontmatter');
        ok(length($ctx->{body_text}) > 0, 'context has body');
        ok(defined $ctx->{related_notes_text}, 'context has related field');

        # Check size bounds
        my $max = $profile->{context_recipe}{max_context_chars};
        ok(length($ctx->{body_text}) <= $max, "body within max_context_chars ($max)");
    }
}

# Digest context
my @mock_log = (
    { pass => 'idea_expand', type => 'idea', title => 'Test Idea', output => 'Some expansion' },
    { pass => 'project_review', type => 'project', title => 'Test Project', output => 'Some review' },
);
my $digest = build_digest_context(\@mock_log);
ok($digest =~ /Test Idea/, 'digest contains idea title');
ok($digest =~ /Test Project/, 'digest contains project title');

# ═══════════════════════════════════════════════
# Test: LlamaRun (prompt building only — no inference)
# ═══════════════════════════════════════════════
section("LlamaRun (prompt building)");

{
    my $profile_path = File::Spec->catfile($Bin, '..', 'profiles', 'idea_expand.json');
    if (-f $profile_path) {
        open my $fh, '<', $profile_path or die $!;
        my $profile = $JSON->decode(do { local $/; <$fh> });
        close $fh;

        my $mock_ctx = {
            frontmatter_yaml   => "type: idea\ntitle: Test",
            body_text          => "This is a test idea about widgets.",
            related_notes_text => "--- Related: Concept X ---\nSummary here.",
        };

        my $prompt = build_prompt($profile, $mock_ctx);
        ok(defined $prompt->{system}, 'prompt has system');
        ok(defined $prompt->{user}, 'prompt has user');
        ok(defined $prompt->{output_contract}, 'prompt has output_contract');
        ok($prompt->{user} =~ /Test/, 'user prompt contains context');
        ok($prompt->{user} =~ /Related/, 'user prompt contains related notes');
        ok($prompt->{system} =~ /venture architect/i, 'system role is correct');
    }
}

# ═══════════════════════════════════════════════
# Test: VaultWrite
# ═══════════════════════════════════════════════
section("VaultWrite");

{
    my $tmpdir = tempdir(CLEANUP => 1);

    # Test append
    my $test_note = File::Spec->catfile($tmpdir, 'test.md');
    open my $fh, '>', $test_note or die $!;
    print $fh "---\ntype: idea\ntitle: Test\n---\n\n# Test Note\n\nBody here.\n";
    close $fh;

    my $result = append_to_note($test_note, '## AI Expansion — 2026-04-03', 'Expanded content here.');
    ok(-f $result, 'append returns filepath');

    open $fh, '<', $test_note or die $!;
    my $content = do { local $/; <$fh> };
    close $fh;
    ok($content =~ /AI Expansion/, 'appended section exists');
    ok($content =~ /Body here/, 'original content preserved');

    # Test duplicate prevention
    my $result2 = append_to_note($test_note, '## AI Expansion — 2026-04-03', 'Double run.');
    open $fh, '<', $test_note or die $!;
    $content = do { local $/; <$fh> };
    close $fh;
    my @matches = ($content =~ /AI Expansion/g);
    ok(scalar @matches == 1, 'duplicate section prevented');

    # Test create
    my $new_note = File::Spec->catfile($tmpdir, 'new-note.md');
    my $fm = { type => 'agent-brief', title => 'Test Brief', created => '2026-04-03' };
    create_note($new_note, $fm, "# Brief\n\nContent here.");
    ok(-f $new_note, 'new note created');

    open $fh, '<', $new_note or die $!;
    $content = do { local $/; <$fh> };
    close $fh;
    ok($content =~ /^---/, 'has frontmatter delimiter');
    ok($content =~ /type: agent-brief/, 'frontmatter written');
    ok($content =~ /Content here/, 'body written');

    # Test overwrite protection
    eval { create_note($new_note, $fm, "Overwrite attempt.") };
    open $fh, '<', $new_note or die $!;
    $content = do { local $/; <$fh> };
    close $fh;
    ok($content =~ /Content here/, 'overwrite prevented');
}

# ═══════════════════════════════════════════════
# PiperTTS (clean_for_speech)
# ═══════════════════════════════════════════════
section("PiperTTS (clean_for_speech)");

{
    # Strip YAML frontmatter
    my $with_fm = "---\ntitle: Test\nstatus: active\n---\nHello world.";
    my $cleaned = clean_for_speech($with_fm);
    ok($cleaned eq 'Hello world.', 'strips frontmatter');

    # Strip markdown headings (keep text)
    ok(clean_for_speech("## My Heading\nBody") =~ /^My Heading/, 'strips heading markers');

    # Strip wiki links (keep display text)
    ok(clean_for_speech("See [[Target|Display Name]] here") eq 'See Display Name here', 'wiki link with display');
    ok(clean_for_speech("See [[Target]] here") eq 'See Target here', 'bare wiki link');

    # Strip bold/italic
    ok(clean_for_speech("This is **bold** and *italic*") eq 'This is bold and italic', 'strips bold/italic');

    # Strip bullet markers
    ok(clean_for_speech("- Item one\n- Item two") eq "Item one\nItem two", 'strips bullets');

    # Strip code blocks
    ok(clean_for_speech("Before `inline code` after") eq 'Before inline code after', 'strips inline code');

    # Strip markdown links
    ok(clean_for_speech("Click [here](http://example.com)") eq 'Click here', 'strips markdown links');

    # Multiple blank lines collapsed
    ok(clean_for_speech("Line one.\n\n\n\nLine two.") eq "Line one.\n\nLine two.", 'collapses blank lines');

    # Blockquote markers
    ok(clean_for_speech("> Quoted text") eq 'Quoted text', 'strips blockquote markers');
}

# ═══════════════════════════════════════════════
# Queue Tests
# ═══════════════════════════════════════════════
{
    section('NightlyQueue');

    # Use a temp vault root so we don't touch the real queue
    my $tmp_vault = tempdir(CLEANUP => 1);

    # Empty queue loads cleanly
    my $q = load_queue($tmp_vault);
    ok(ref($q) eq 'HASH', 'load_queue returns hashref');
    ok(ref($q->{items}) eq 'ARRAY' && @{$q->{items}} == 0, 'empty queue has no items');

    # Enqueue a pass
    my $entry = enqueue_pass($tmp_vault, 'idea_expand', limit => 2, no_audio => 1);
    ok($entry->{pass_name} eq 'idea_expand', 'enqueue sets pass_name');
    ok($entry->{status} eq 'queued', 'enqueue sets status to queued');
    ok($entry->{limit} == 2, 'enqueue preserves limit option');

    # Duplicate enqueue returns existing entry
    my $dup = enqueue_pass($tmp_vault, 'idea_expand');
    ok($dup->{job_id} eq $entry->{job_id}, 'duplicate enqueue returns same job');

    # Enqueue a second pass
    enqueue_pass($tmp_vault, 'project_review');
    my $status = queue_status($tmp_vault);
    ok($status->{total_items} == 2, 'queue has 2 items after 2 enqueues');
    ok($status->{counts}{queued} == 2, 'both items are queued');

    # Next queued item returns first
    my $next = next_queued_item($tmp_vault);
    ok($next->{pass_name} eq 'idea_expand', 'next_queued_item returns first queued');

    # Mark processing
    my $marked = mark_queue_item($tmp_vault, $entry->{job_id}, 'processing');
    ok($marked->{status} eq 'processing', 'mark_queue_item sets processing');
    ok(defined $marked->{started_at}, 'processing sets started_at');
    ok($marked->{attempt_count} == 1, 'attempt_count incremented');

    # Next queued skips processing items
    $next = next_queued_item($tmp_vault);
    ok($next->{pass_name} eq 'project_review', 'next_queued skips processing items');

    # Mark completed
    $marked = mark_queue_item($tmp_vault, $entry->{job_id}, 'completed');
    ok($marked->{status} eq 'completed', 'mark completed works');
    ok(defined $marked->{completed_at}, 'completed sets completed_at');

    # Mark failed
    my $pr_entry = next_queued_item($tmp_vault);
    mark_queue_item($tmp_vault, $pr_entry->{job_id}, 'failed', error => 'test error');
    my $final_status = queue_status($tmp_vault);
    ok($final_status->{counts}{completed} == 1, 'one completed in final status');
    ok($final_status->{counts}{failed} == 1, 'one failed in final status');

    # Queue file exists on disk
    ok(-f queue_path($tmp_vault), 'queue file created on disk');

    # Cleanup
    remove_tree($tmp_vault);
}

# ═══════════════════════════════════════════════
# Summary
# ═══════════════════════════════════════════════
print "\n═══════════════════════════════════════════════\n";
print "  $tests tests, $pass passed, $fail failed\n";
print "═══════════════════════════════════════════════\n";

exit($fail > 0 ? 1 : 0);
