#!/usr/bin/env perl
# ═══════════════════════════════════════════════════════════════
# nightly.pl — Nightly Batch Cognition Orchestrator
# ═══════════════════════════════════════════════════════════════
# Runs specialized AI passes over selected note bundles overnight.
# Each pass has a role, a bounded context recipe, and a strict output.
# Simple routing, advanced prompts. The Perl stays boring.
#
# Usage:
#   perl nightly.pl                         # Run all passes
#   perl nightly.pl --pass idea_expand      # Run one pass
#   perl nightly.pl --dry-run               # Test without llama.cpp
#   perl nightly.pl --pass agent_brief --verbose
#
# ═══════════════════════════════════════════════════════════════

use strict;
use warnings;
use utf8;
use FindBin qw($Bin);
use lib "$Bin/lib";
use File::Spec;
use File::Path qw(make_path);
use JSON::PP;
use POSIX qw(strftime ceil);
use Getopt::Long;
use Fcntl qw(:flock);

use VaultParse  qw(parse_note);
use VaultScan   qw(scan_vault find_notes find_notes_by_profile);
use ContextBundle qw(build_context build_digest_context);
use LlamaRun    qw(run_inference build_prompt load_config);
use VaultWrite  qw(write_output);
use PiperTTS    qw(synthesize synthesize_note_audio clean_for_speech);
use NightlyQueue qw(load_queue save_queue enqueue_pass next_queued_item mark_queue_item queue_status);

my $JSON = JSON::PP->new->utf8->pretty->canonical;

# ── Shared State ──
our $all_notes;

# ── CLI Options ──
my $opt_pass;
my $opt_dry_run;
my $opt_verbose;
my $opt_config;
my $opt_limit;
my $opt_no_audio;
my $opt_unsafe_skip_guardrails;
my $opt_enqueue_pass;
my $opt_queue_status;
my $opt_process_queued;
my $opt_max_queued_jobs;

GetOptions(
    'pass=s'    => \$opt_pass,
    'dry-run'   => \$opt_dry_run,
    'verbose'   => \$opt_verbose,
    'config=s'  => \$opt_config,
    'limit=i'   => \$opt_limit,
    'no-audio'  => \$opt_no_audio,
    'unsafe-skip-guardrails' => \$opt_unsafe_skip_guardrails,
    'enqueue-pass=s'   => \$opt_enqueue_pass,
    'queue-status'     => \$opt_queue_status,
    'process-queued'   => \$opt_process_queued,
    'max-queued-jobs=i' => \$opt_max_queued_jobs,
) or die "Usage: perl nightly.pl [--pass NAME] [--dry-run] [--verbose] [--no-audio] [--config PATH] [--limit N] [--unsafe-skip-guardrails] [--enqueue-pass NAME] [--queue-status] [--process-queued] [--max-queued-jobs N]\n";

# ── Load Config ──
my $config_path = $opt_config || "$Bin/nightly.json";
my $config = load_config($config_path);

# Override dry_run if flag passed
$config->{dry_run} = 1 if $opt_dry_run;

my $vault_root = $config->{vault_root} || File::Spec->catdir($Bin, '..', '..');
# If the configured vault_root is a relative path, resolve it against the
# script location ($Bin) so the program behaves consistently regardless of
# the current working directory used to invoke the script.
if (File::Spec->file_name_is_absolute($vault_root)) {
    $vault_root = File::Spec->rel2abs($vault_root);
} else {
    $vault_root = File::Spec->rel2abs(File::Spec->catfile($Bin, $vault_root));
}

# ── Queue Commands (exit early, no inference needed) ──
if ($opt_queue_status) {
    my $status = queue_status($vault_root);
    print $JSON->encode($status);
    exit 0;
}

if ($opt_enqueue_pass) {
    my $entry = enqueue_pass($vault_root, $opt_enqueue_pass,
        limit    => $opt_limit,
        no_audio => $opt_no_audio,
    );
    print $JSON->encode($entry);
    exit 0;
}

if ($opt_process_queued) {
    my $runtime_lock_fh = guard_runtime($vault_root, $config, $opt_unsafe_skip_guardrails);
    my $max_jobs = $opt_max_queued_jobs || 3;
    my $processed = 0;

    # Scan vault once and reuse across all queued jobs
    $all_notes = scan_vault($vault_root,
        '01-Ideas', '02-Projects', '03-Research',
        '04-Systems', '05-Monetization', '06-Logs',
        '07-Agent Briefs'
    );

    for (1 .. $max_jobs) {
        my $item = next_queued_item($vault_root);
        last unless $item;

        my $pass_name = $item->{pass_name};
        my $job_id    = $item->{job_id};
        log_msg("Processing queued job: $job_id ($pass_name)");
        mark_queue_item($vault_root, $job_id, 'processing');

        eval {
            # Set options from queued item for the pass runners
            $opt_limit    = $item->{limit} if defined $item->{limit};
            $opt_no_audio = 1 if $item->{no_audio};

            # Run the pass through the normal execution path
            my $profile = load_profile($pass_name);
            die "No profile found for '$pass_name'\n" unless $profile;

            if ($pass_name eq 'morning_digest') {
                run_digest_pass($profile);
            } elsif ($pass_name eq 'morning_brief_audio') {
                run_morning_brief_pass($profile);
            } else {
                run_standard_pass($pass_name, $profile);
            }

            mark_queue_item($vault_root, $job_id, 'completed');
            $processed++;
        };
        if ($@) {
            log_msg("Queue job FAILED: $@");
            mark_queue_item($vault_root, $job_id, 'failed', error => "$@");
        }
    }

    log_msg("Queue drain complete: $processed jobs processed");
    exit 0;
}

my $runtime_lock_fh = guard_runtime($vault_root, $config, $opt_unsafe_skip_guardrails);

my $date = strftime('%Y-%m-%d', localtime);
my $time_start = time();

# ── Define Pass Order ──
my @default_pass_order = qw(
    idea_expand
    project_review
    system_wire
    agent_brief
    morning_digest
    morning_brief_audio
    project_narrator
    idea_playback
    distribution_snippet
);

my @passes = $opt_pass ? ($opt_pass) : @default_pass_order;

# ── Pre-scan the vault once (shared across passes) ──
log_msg("Scanning vault: $vault_root");
$all_notes = scan_vault($vault_root,
    '01-Ideas', '02-Projects', '03-Research',
    '04-Systems', '05-Monetization', '06-Logs',
    '07-Agent Briefs'
);
log_msg("Found " . scalar(@$all_notes) . " notes with frontmatter");

# ── Run Log (for morning digest) ──
my @run_log;

# ── Execute Passes ──
for my $pass_name (@passes) {
    log_msg("═" x 50);
    log_msg("PASS: $pass_name");
    log_msg("═" x 50);

    my $profile = load_profile($pass_name);
    unless ($profile) {
        log_msg("  SKIP: no profile found for '$pass_name'");
        next;
    }

    if ($pass_name eq 'morning_digest') {
        run_digest_pass($profile);
        next;
    }

    if ($pass_name eq 'morning_brief_audio') {
        run_morning_brief_pass($profile);
        next;
    }

    run_standard_pass($pass_name, $profile);
}

# ── Summary ──
my $elapsed = time() - $time_start;
log_msg("═" x 50);
log_msg("COMPLETE — $date — ${elapsed}s elapsed");
log_msg("  Notes processed: " . scalar(@run_log));
log_msg("  Passes run: " . join(', ', @passes));
log_msg("═" x 50);

# Write run log to disk
write_run_log();

exit 0;

# ═══════════════════════════════════════════════════════════════
# Runtime Guardrails
# ═══════════════════════════════════════════════════════════════

sub guard_runtime {
    my ($vault_root, $config, $unsafe_skip) = @_;
    return undef if $unsafe_skip;

    my $guardrail_cfg = load_guardrail_config($vault_root);
    my $limit = $guardrail_cfg->{process_limits}{nightly_brainstorm}
             || $config->{safety}{max_load_avg}
             || $guardrail_cfg->{default_max_load_avg}
             || recommended_load_limit();
    my $current = current_load_average();
    if (defined $current && $current > $limit) {
        die sprintf(
            "Refusing to start nightly_brainstorm: current 1m load %.2f exceeds safe limit %.2f. Use --unsafe-skip-guardrails to override.\n",
            $current, $limit
        );
    }

    my $runtime_dir = File::Spec->catdir($vault_root, '.bonfyre-runtime');
    make_path($runtime_dir) unless -d $runtime_dir;
    my $lock_path = File::Spec->catfile($runtime_dir, 'heavy-process.lock');

    open my $fh, '+>>', $lock_path or die "Cannot open runtime lock $lock_path: $!\n";
    unless (flock($fh, LOCK_EX | LOCK_NB)) {
        seek($fh, 0, 0);
        my $existing = do { local $/; <$fh> } // '';
        chomp $existing;
        my $detail = $existing ? " Existing lock metadata: $existing" : '';
        die "Refusing to start nightly_brainstorm: another heavy Bonfyre process is already running.$detail\n";
    }

    seek($fh, 0, 0);
    truncate($fh, 0);
    print {$fh} $JSON->encode({
        process_name => 'nightly_brainstorm',
        pid          => $$,
        started_at   => time(),
    });

    return $fh;
}

sub current_load_average {
    my $raw = qx{/usr/sbin/sysctl -n vm.loadavg 2>/dev/null};
    if ($raw =~ /\{\s*([0-9.]+)/) {
        return $1 + 0;
    }
    return undef;
}

sub load_guardrail_config {
    my ($vault_root) = @_;
    my $default = {
        default_max_load_avg => 12,
        process_limits => {
            nightly_brainstorm => 8,
            local_ai_transcription_service => 12,
            'local_ai_transcription_service:model_warmup' => 5,
        },
    };

    my $config_path = File::Spec->catfile($vault_root, '.bonfyre-runtime', 'guardrails.json');
    return $default unless -f $config_path;

    my $raw = eval {
        open my $fh, '<', $config_path or die $!;
        local $/;
        <$fh>;
    };
    return $default if $@ || !$raw;

    my $decoded = eval { decode_json($raw) };
    return $default if $@ || ref($decoded) ne 'HASH';

    if (ref($decoded->{process_limits}) eq 'HASH') {
        $default->{process_limits}{$_} = $decoded->{process_limits}{$_}
            for keys %{ $decoded->{process_limits} };
    }
    $default->{default_max_load_avg} = $decoded->{default_max_load_avg}
        if defined $decoded->{default_max_load_avg};

    return $default;
}

sub recommended_load_limit {
    my $cpu = qx{/usr/sbin/sysctl -n hw.logicalcpu 2>/dev/null};
    chomp $cpu;
    $cpu = 8 unless $cpu && $cpu =~ /^\d+$/;
    my $limit = $cpu * 1.5;
    return $limit < 8 ? 8 : ceil($limit);
}

# ═══════════════════════════════════════════════════════════════
# Pass Runners
# ═══════════════════════════════════════════════════════════════

sub run_standard_pass {
    my ($pass_name, $profile) = @_;

    my $notes = find_notes_by_profile($vault_root, $profile);
    log_msg("  Selected " . scalar(@$notes) . " notes");

    if ($opt_limit && @$notes > $opt_limit) {
        log_msg("  Limiting to $opt_limit notes");
        splice @$notes, $opt_limit;
    }

    my $processed = 0;
    for my $note (@$notes) {
        my $title = $note->{frontmatter}{title} // $note->{filename};
        log_msg("  ── $title ──");

        eval {
            # 1. Build context bundle
            my $context = build_context($note, $profile, $vault_root, $all_notes);

            if ($opt_verbose) {
                log_msg("    Context: " . length($context->{body_text}) . " chars body, "
                    . length($context->{related_notes_text} // '') . " chars related");
            }

            # 2. Build prompt
            my $prompt_parts = build_prompt($profile, $context);

            if ($opt_verbose) {
                my $total = length($prompt_parts->{system})
                          + length($prompt_parts->{user})
                          + length($prompt_parts->{output_contract});
                log_msg("    Prompt: $total chars total");
            }

            # 3. Run inference
            log_msg("    Running inference ($profile->{inference}{temperature} temp)...");
            my $output = run_inference($config, $profile, $prompt_parts);

            if ($opt_verbose) {
                log_msg("    Output: " . length($output) . " chars");
            }

            # 4. Write output (skip if audio-only pass or dry-run)
            my $written_path;
            if ($config->{dry_run}) {
                $written_path = '(dry run — skipped write)';
            } elsif (($profile->{output}{mode} || '') eq 'skip_text') {
                $written_path = '(audio only)';
            } else {
                $written_path = write_output($profile, $note, $output, $vault_root);
                log_msg("    Written: $written_path");
            }

            # 5. Audio synthesis (if profile has audio config)
            if (!$opt_no_audio && $profile->{output}{audio} && $profile->{output}{audio}{enabled}) {
                my $piper_cfg = _piper_config();
                if ($piper_cfg) {
                    my $audio_cat = $profile->{output}{audio}{category} || 'Daily';
                    log_msg("    Synthesizing audio ($audio_cat)...");
                    my $audio_path = synthesize_note_audio(
                        $output, $audio_cat, $title, $vault_root, $piper_cfg
                    );
                    if ($audio_path) {
                        log_msg("    Audio: $audio_path");
                    } else {
                        log_msg("    Audio: skipped (empty or failed)");
                    }
                }
            }

            # 6. Log for digest
            push @run_log, {
                pass   => $pass_name,
                type   => $note->{frontmatter}{type},
                title  => $title,
                path   => $written_path,
                output => $output,
            };

            $processed++;
        };
        if ($@) {
            log_msg("    ERROR: $@");
            push @run_log, {
                pass   => $pass_name,
                type   => $note->{frontmatter}{type},
                title  => $title,
                path   => $note->{path},
                error  => "$@",
            };
        }
    }

    log_msg("  Pass complete: $processed/" . scalar(@$notes) . " notes processed");
}

sub run_digest_pass {
    my ($profile) = @_;

    if (!@run_log) {
        log_msg("  SKIP: no run log entries to digest");
        return;
    }

    log_msg("  Digesting " . scalar(@run_log) . " entries");

    eval {
        # Build digest context from run log
        my $digest_text = build_digest_context(\@run_log);

        # Build a synthetic "note" for the digest
        my $digest_note = {
            frontmatter => { title => "Morning Review", type => 'log' },
            filename    => "Morning Review — $date",
            path        => '',
        };

        my $context = {
            frontmatter_yaml   => "date: $date\nnotes_processed: " . scalar(@run_log),
            body_text          => '',
            related_notes_text => '',
            digest_entries     => $digest_text,
        };

        my $prompt_parts = build_prompt($profile, $context);
        log_msg("    Running digest inference...");
        my $output = run_inference($config, $profile, $prompt_parts);

        if ($config->{dry_run}) {
            log_msg("    (dry run — skipped write)");
        } else {
            my $written = write_output($profile, $digest_note, $output, $vault_root);
            log_msg("    Written: $written");
        }
    };
    if ($@) {
        log_msg("    DIGEST ERROR: $@");
    }
}

sub run_morning_brief_pass {
    my ($profile) = @_;

    log_msg("  Building morning brief context...");

    eval {
        # Gather active projects
        my $projects = find_notes($vault_root, {
            type => 'project', status => ['active'], paths => ['02-Projects/']
        });

        # Build context from projects + run log
        my @brief_parts;
        for my $p (@$projects) {
            my $title  = $p->{frontmatter}{title} // $p->{filename};
            my $status = $p->{frontmatter}{status} // 'unknown';
            my $priority = $p->{frontmatter}{priority} // 'unknown';
            my $body = VaultParse::section_text($p, 'Summary', 'Next Action', 'Bottlenecks', 'First Deliverable');
            push @brief_parts, "Project: $title (status: $status, priority: $priority)\n$body";
        }

        # Add run log entries if any
        if (@run_log) {
            push @brief_parts, "\n--- Overnight AI Activity ---";
            for my $entry (@run_log) {
                my $t = $entry->{title} // 'untitled';
                my $pass = $entry->{pass} // 'unknown';
                push @brief_parts, "[$pass] $t";
            }
        }

        my $digest_text = join("\n\n", @brief_parts);

        my $brief_note = {
            frontmatter => { title => "Morning Brief", type => 'log' },
            filename    => "Morning Brief — $date",
            path        => '',
        };

        my $context = {
            frontmatter_yaml   => "date: $date",
            body_text          => '',
            related_notes_text => '',
            digest_entries     => $digest_text,
        };

        # Replace {{date}} in user template
        $context->{_date} = $date;

        my $prompt_parts = build_prompt($profile, $context);
        # Manual date replacement in user prompt
        $prompt_parts->{user} =~ s/\{\{date\}\}/$date/g;

        log_msg("    Running brief inference...");
        my $output = run_inference($config, $profile, $prompt_parts);

        # Write text note
        my $written;
        if ($config->{dry_run}) {
            $written = '(dry run — skipped write)';
            log_msg("    (dry run — skipped write)");
        } else {
            $written = write_output($profile, $brief_note, $output, $vault_root);
            log_msg("    Written: $written");
        }

        # Synthesize audio
        if (!$opt_no_audio) {
            my $piper_cfg = _piper_config();
            if ($piper_cfg) {
                log_msg("    Synthesizing morning brief audio...");
                my $audio_path = synthesize_note_audio(
                    $output, 'Daily', "Morning-Brief", $vault_root, $piper_cfg
                );
                if ($audio_path) {
                    log_msg("    Audio: $audio_path");
                } else {
                    log_msg("    Audio: skipped (empty or failed)");
                }
            }
        }

        push @run_log, {
            pass   => 'morning_brief_audio',
            type   => 'log',
            title  => 'Morning Brief',
            path   => $written,
            output => $output,
        };
    };
    if ($@) {
        log_msg("    BRIEF ERROR: $@");
    }
}

# ═══════════════════════════════════════════════════════════════
# Utilities
# ═══════════════════════════════════════════════════════════════

sub _piper_config {
    return undef if $config->{dry_run};
    my $bin = $config->{piper_bin};
    my $model = $config->{piper_model};
    return undef unless $bin && $model && -x $bin && -f $model;
    return {
        piper_bin        => $bin,
        piper_model      => $model,
        sentence_silence => $config->{sentence_silence} // '0.3',
        volume           => $config->{volume} // '1.0',
    };
}

sub load_profile {
    my ($name) = @_;
    my $path = File::Spec->catfile($Bin, 'profiles', "$name.json");

    unless (-f $path) {
        warn "Profile not found: $path\n";
        return undef;
    }

    open my $fh, '<:raw', $path or die "Cannot open $path: $!\n";
    my $raw = do { local $/; <$fh> };
    close $fh;

    return $JSON->decode($raw);
}

sub write_run_log {
    my $log_dir = File::Spec->catdir($Bin, 'logs');
    mkdir $log_dir unless -d $log_dir;

    my $log_path = File::Spec->catfile($log_dir, "run-$date.json");

    # Don't include full output in run log file (too large)
    my @log_entries = map {
        my %entry = %$_;
        $entry{output_length} = length($entry{output} // '');
        delete $entry{output};
        \%entry;
    } @run_log;

    my $log_data = {
        date     => $date,
        elapsed  => time() - $time_start,
        passes   => \@passes,
        dry_run  => $config->{dry_run} ? JSON::PP::true : JSON::PP::false,
        entries  => \@log_entries,
    };

    open my $fh, '>:encoding(UTF-8)', $log_path
        or warn "Cannot write run log: $!\n" and return;
    print $fh $JSON->encode($log_data);
    close $fh;

    log_msg("Run log: $log_path");
}

sub log_msg {
    my ($msg) = @_;
    my $ts = strftime('%H:%M:%S', localtime);
    binmode(STDOUT, ':encoding(UTF-8)') unless $ENV{_BINMODE_SET};
    $ENV{_BINMODE_SET} = 1;
    print "[$ts] $msg\n";
}
