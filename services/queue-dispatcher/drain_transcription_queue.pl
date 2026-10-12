#!/usr/bin/env perl
# ═══════════════════════════════════════════════════════════════
# drain_all_queues.pl — Unified Bonfyre queue dispatcher
# ═══════════════════════════════════════════════════════════════
# Coordinates both NightlyBrainstorm and LocalAITranscriptionService
# queues. Uses its own dispatcher lock to prevent overlapping drain
# cycles, then calls each service's --process-queued in priority
# order. Each service acquires heavy-process.lock independently.
#
# Architecture:
#   dispatcher.lock  ← this script (prevents two dispatchers)
#   heavy-process.lock ← each child (prevents two heavy jobs)
#
# Priority: transcription (customer-facing) > nightly brainstorm
#
# Usage:
#   perl drain_all_queues.pl                    # drain up to defaults
#   perl drain_all_queues.pl --dry-run          # show what would run
#   perl drain_all_queues.pl --max-jobs 5       # override total cap
#   perl drain_all_queues.pl --status           # show both queue counts
#   perl drain_all_queues.pl --unsafe-skip-guardrails  # bypass load check
# ═══════════════════════════════════════════════════════════════

use strict;
use warnings;
use utf8;
use FindBin qw($Bin);
use Cwd;
use File::Spec;
use File::Path qw(make_path);
use JSON::PP;
use POSIX qw(strftime);
use Getopt::Long;
use Fcntl qw(:flock);

binmode(STDOUT, ':utf8');
binmode(STDERR, ':utf8');

my $JSON = JSON::PP->new->utf8->pretty->canonical;

# ── Resolve vault root ──
# This script lives in .bonfyre-runtime/ — vault root is one level up
my $vault_root = Cwd::abs_path(File::Spec->catdir($Bin, '..'));

my $runtime_dir       = File::Spec->catdir($vault_root, '.bonfyre-runtime');
my $dispatcher_lock   = File::Spec->catfile($runtime_dir, 'dispatcher.lock');
my $guardrail_path    = File::Spec->catfile($runtime_dir, 'guardrails.json');

my $transcription_dir = File::Spec->catdir($vault_root, '10-Code', 'LocalAITranscriptionService');
my $nightly_dir       = File::Spec->catdir($vault_root, '10-Code', 'NightlyBrainstorm');

my $transcription_queue = File::Spec->catfile($runtime_dir, 'local-ai-transcription-queue.json');
my $nightly_queue       = File::Spec->catfile($runtime_dir, 'nightly-brainstorm-queue.json');

# ── CLI Options ──
my $opt_dry_run;
my $opt_max_jobs = 4;
my $opt_unsafe_skip;
my $opt_verbose;
my $opt_status;

GetOptions(
    'dry-run'                => \$opt_dry_run,
    'max-jobs=i'             => \$opt_max_jobs,
    'unsafe-skip-guardrails' => \$opt_unsafe_skip,
    'verbose'                => \$opt_verbose,
    'status'                 => \$opt_status,
) or die "Usage: perl drain_all_queues.pl [--dry-run] [--max-jobs N] [--status] [--unsafe-skip-guardrails] [--verbose]\n";

# ═══════════════════════════════════════════════════════════════
# Helpers (declared early so main can call them)
# ═══════════════════════════════════════════════════════════════

sub log_msg {
    my ($msg) = @_;
    my $ts = strftime('%H:%M:%S', localtime);
    print "[$ts] $msg\n";
}

# ═══════════════════════════════════════════════════════════════
# Guardrails
# ═══════════════════════════════════════════════════════════════

sub current_load_average {
    my $raw = qx{/usr/sbin/sysctl -n vm.loadavg 2>/dev/null};
    return ($raw =~ /\{\s*([0-9.]+)/) ? $1 + 0 : undef;
}

sub load_guardrail_config {
    my $default = { default_max_load_avg => 12 };
    return $default unless -f $guardrail_path;
    my $raw = eval { local $/; open my $fh, '<', $guardrail_path or die $!; <$fh> };
    return $default if $@ || !$raw;
    my $cfg = eval { $JSON->decode($raw) };
    return $default if $@ || ref($cfg) ne 'HASH';
    return $cfg;
}

sub check_guardrails {
    return 1 if $opt_unsafe_skip;

    my $cfg     = load_guardrail_config();
    my $limit   = $cfg->{default_max_load_avg} || 12;
    my $current = current_load_average();

    if (defined $current && $current > $limit) {
        log_msg(sprintf("Load %.2f exceeds limit %.2f — skipping this drain cycle", $current, $limit));
        return 0;
    }
    return 1;
}

# ═══════════════════════════════════════════════════════════════
# Queue Readers
# ═══════════════════════════════════════════════════════════════

sub read_queue_file {
    my ($path) = @_;
    return { items => [] } unless -f $path;
    my $raw = eval { local $/; open my $fh, '<:raw', $path or die $!; <$fh> };
    return { items => [] } if $@ || !$raw;
    my $q = eval { $JSON->decode($raw) };
    return { items => [] } if $@ || ref($q) ne 'HASH';
    $q->{items} = [] unless ref($q->{items}) eq 'ARRAY';
    return $q;
}

sub count_queued {
    my ($path) = @_;
    my $q = read_queue_file($path);
    return scalar grep { ref($_) eq 'HASH' && ($_->{status} // '') eq 'queued' } @{ $q->{items} };
}

# ═══════════════════════════════════════════════════════════════
# Status
# ═══════════════════════════════════════════════════════════════

sub show_status {
    my $t_queued = count_queued($transcription_queue);
    my $n_queued = count_queued($nightly_queue);
    my $load     = current_load_average() // 'unknown';

    print $JSON->encode({
        load_1m            => $load,
        transcription      => { queued => $t_queued, queue_file => $transcription_queue },
        nightly_brainstorm => { queued => $n_queued, queue_file => $nightly_queue },
    });
}

# ═══════════════════════════════════════════════════════════════
# Dispatchers — each child acquires heavy-process.lock on its own
# ═══════════════════════════════════════════════════════════════

sub drain_transcription {
    my ($max) = @_;
    return 0 if $max <= 0;
    return 0 unless -d $transcription_dir;

    my $queued = count_queued($transcription_queue);
    return 0 if $queued == 0;

    my $to_run = $queued < $max ? $queued : $max;
    log_msg("Transcription: $to_run of $queued queued jobs");

    if ($opt_dry_run) {
        log_msg("  [dry run] would process $to_run transcription jobs");
        return $to_run;
    }

    my @cmd = (
        'python3', '-m', 'local_ai_transcription_service.cli',
        '--process-queued',
        '--max-queued-jobs', $to_run,
        '--output-root', 'outputs',
    );
    push @cmd, '--unsafe-skip-guardrails' if $opt_unsafe_skip;

    my $saved = Cwd::getcwd();
    chdir $transcription_dir or do {
        log_msg("  Cannot chdir to $transcription_dir: $!");
        return 0;
    };

    $ENV{PYTHONPATH} = 'src';
    log_msg("  Running: " . join(' ', @cmd)) if $opt_verbose;
    my $exit = system(@cmd);
    chdir $saved;
    delete $ENV{PYTHONPATH};

    if ($exit != 0) {
        my $code = $exit >> 8;
        # Exit code from lock contention (RuntimeError) — not an error, just busy
        log_msg("  Transcription exited $code (lock contention or runtime error)");
        return 0;
    }

    return $to_run;
}

sub drain_nightly {
    my ($max) = @_;
    return 0 if $max <= 0;
    return 0 unless -d $nightly_dir;

    my $queued = count_queued($nightly_queue);
    return 0 if $queued == 0;

    my $to_run = $queued < $max ? $queued : $max;
    log_msg("Nightly: $to_run of $queued queued jobs");

    if ($opt_dry_run) {
        log_msg("  [dry run] would process $to_run nightly jobs");
        return $to_run;
    }

    my @cmd = (
        'perl', 'nightly.pl',
        '--process-queued',
        '--max-queued-jobs', $to_run,
        '--no-audio',
    );
    push @cmd, '--unsafe-skip-guardrails' if $opt_unsafe_skip;

    my $saved = Cwd::getcwd();
    chdir $nightly_dir or do {
        log_msg("  Cannot chdir to $nightly_dir: $!");
        return 0;
    };

    log_msg("  Running: " . join(' ', @cmd)) if $opt_verbose;
    my $exit = system(@cmd);
    chdir $saved;

    if ($exit != 0) {
        my $code = $exit >> 8;
        log_msg("  Nightly exited $code (lock contention or runtime error)");
        return 0;
    }

    return $to_run;
}

# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

if ($opt_status) {
    show_status();
    exit 0;
}

log_msg("═" x 50);
log_msg("Bonfyre Queue Dispatcher — pid $$");
log_msg("═" x 50);

# 1. Pre-flight: anything to drain?
my $t_pending = count_queued($transcription_queue);
my $n_pending = count_queued($nightly_queue);

if ($t_pending == 0 && $n_pending == 0) {
    log_msg("No queued jobs in either queue — nothing to do");
    exit 0;
}

log_msg("Pending: $t_pending transcription, $n_pending nightly");

# 2. Check load guardrails (exit cleanly if too hot)
exit 0 unless check_guardrails();

# 3. Acquire dispatcher lock (prevents overlapping drain cycles)
make_path($runtime_dir) unless -d $runtime_dir;

my $lock_fh;
unless ($opt_dry_run) {
    open $lock_fh, '+>>', $dispatcher_lock or die "Cannot open $dispatcher_lock: $!\n";
    unless (flock($lock_fh, LOCK_EX | LOCK_NB)) {
        log_msg("Another dispatcher is already running — skipping");
        exit 0;
    }
    seek($lock_fh, 0, 0);
    truncate($lock_fh, 0);
    print {$lock_fh} $JSON->encode({
        process_name => 'drain_all_queues',
        pid          => $$,
        started_at   => time(),
    });
}

# 4. Drain queues in priority order
#    Each child acquires heavy-process.lock independently.
#    If the first child grabs the lock and runs, the second may find
#    load elevated — that's fine, its own guardrails will gate it.
my $total = 0;
my $budget = $opt_max_jobs;

# Priority 1: Transcription (customer-facing, capped at 2 per cycle)
my $t_cap = $budget > 2 ? 2 : $budget;
my $t_done = drain_transcription($t_cap);
$total  += $t_done;
$budget -= $t_done;

# Sync transcription notes into vault for Obsidian Bases
if ($t_done > 0) {
    log_msg("Syncing transcription notes to 08-Transcriptions/...");
    my $sync_script = File::Spec->catfile($runtime_dir, 'sync-transcription-notes.py');
    if (-f $sync_script) {
        system('python3', $sync_script);
    }
}

# Re-check load between services if transcription ran
if ($t_done > 0 && !$opt_unsafe_skip) {
    my $load = current_load_average();
    my $cfg  = load_guardrail_config();
    my $limit = $cfg->{process_limits}{nightly_brainstorm} || $cfg->{default_max_load_avg} || 12;
    if (defined $load && $load > $limit) {
        log_msg(sprintf("Post-transcription load %.2f exceeds nightly limit %.2f — deferring nightly", $load, $limit));
        $budget = 0;
    }
}

# Priority 2: Nightly brainstorm
my $n_done = drain_nightly($budget);
$total += $n_done;

# 5. Release dispatcher lock
if ($lock_fh) {
    seek($lock_fh, 0, 0);
    truncate($lock_fh, 0);
    close $lock_fh;
}

log_msg("═" x 50);
log_msg("Done: $total jobs ($t_done transcription, $n_done nightly)");
log_msg("═" x 50);

exit 0;
