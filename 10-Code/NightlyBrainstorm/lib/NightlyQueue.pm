package NightlyQueue;
# NightlyQueue — File-backed job queue for nightly brainstorm passes.
# Mirrors the Python queue.py pattern used by LocalAITranscriptionService.
# Queue file lives at .bonfyre-runtime/nightly-brainstorm-queue.json

use strict;
use warnings;
use utf8;
use JSON::PP;
use File::Spec;
use File::Path qw(make_path);
use POSIX qw(strftime);
use Exporter 'import';

our @EXPORT_OK = qw(
    load_queue save_queue enqueue_pass
    next_queued_item mark_queue_item
    queue_status queue_path
);

my $JSON = JSON::PP->new->utf8->pretty->canonical;

sub _utc_now {
    return strftime('%Y-%m-%dT%H:%M:%S+00:00', gmtime);
}

sub _runtime_dir {
    my ($vault_root) = @_;
    return File::Spec->catdir($vault_root, '.bonfyre-runtime');
}

sub queue_path {
    my ($vault_root) = @_;
    return File::Spec->catfile(_runtime_dir($vault_root), 'nightly-brainstorm-queue.json');
}

sub _empty_queue {
    return {
        created_at => _utc_now(),
        updated_at => _utc_now(),
        items      => [],
    };
}

sub load_queue {
    my ($vault_root) = @_;
    my $path = queue_path($vault_root);
    return _empty_queue() unless -f $path;

    my $raw = eval {
        open my $fh, '<:raw', $path or die $!;
        local $/;
        <$fh>;
    };
    return _empty_queue() if $@ || !$raw;

    my $payload = eval { $JSON->decode($raw) };
    return _empty_queue() if $@ || ref($payload) ne 'HASH';

    $payload->{items} = [] unless ref($payload->{items}) eq 'ARRAY';
    return $payload;
}

sub save_queue {
    my ($vault_root, $payload) = @_;
    my $dir = _runtime_dir($vault_root);
    make_path($dir) unless -d $dir;

    $payload->{updated_at} = _utc_now();
    my $path = queue_path($vault_root);
    open my $fh, '>:raw', $path or die "Cannot write queue $path: $!\n";
    print $fh $JSON->encode($payload);
    close $fh;
    return $path;
}

sub enqueue_pass {
    my ($vault_root, $pass_name, %opts) = @_;
    my $queue = load_queue($vault_root);
    my $items = $queue->{items};

    # Don't duplicate if same pass is already queued
    for my $item (@$items) {
        if (ref($item) eq 'HASH'
            && ($item->{pass_name} // '') eq $pass_name
            && ($item->{status} // '') =~ /^(?:queued|processing)$/) {
            return $item;
        }
    }

    my $entry = {
        job_id       => "nightly-$pass_name-" . strftime('%Y%m%d%H%M%S', localtime),
        pass_name    => $pass_name,
        status       => 'queued',
        limit        => $opts{limit},
        no_audio     => $opts{no_audio} ? JSON::PP::true : JSON::PP::false,
        queued_at    => _utc_now(),
        started_at   => undef,
        completed_at => undef,
        failed_at    => undef,
        attempt_count => 0,
        last_error   => undef,
    };

    push @$items, $entry;
    $queue->{items} = $items;
    save_queue($vault_root, $queue);
    return $entry;
}

sub next_queued_item {
    my ($vault_root) = @_;
    my $queue = load_queue($vault_root);
    for my $item (@{ $queue->{items} }) {
        next unless ref($item) eq 'HASH';
        return $item if ($item->{status} // '') eq 'queued';
    }
    return undef;
}

sub mark_queue_item {
    my ($vault_root, $job_id, $new_status, %opts) = @_;
    my $queue = load_queue($vault_root);

    for my $item (@{ $queue->{items} }) {
        next unless ref($item) eq 'HASH';
        next unless ($item->{job_id} // '') eq $job_id;

        $item->{status} = $new_status;
        if ($new_status eq 'processing') {
            $item->{started_at} = _utc_now();
            $item->{attempt_count} = ($item->{attempt_count} || 0) + 1;
            $item->{last_error} = undef;
        } elsif ($new_status eq 'completed') {
            $item->{completed_at} = _utc_now();
        } elsif ($new_status eq 'failed') {
            $item->{failed_at} = _utc_now();
            $item->{last_error} = $opts{error};
        }

        save_queue($vault_root, $queue);
        return $item;
    }

    die "Queue item not found: $job_id\n";
}

sub queue_status {
    my ($vault_root) = @_;
    my $queue = load_queue($vault_root);
    my @items = grep { ref($_) eq 'HASH' } @{ $queue->{items} };

    my %counts = (queued => 0, processing => 0, completed => 0, failed => 0);
    for my $item (@items) {
        my $s = $item->{status} // 'queued';
        $counts{$s} = ($counts{$s} || 0) + 1;
    }

    return {
        queue_path  => queue_path($vault_root),
        total_items => scalar(@items),
        counts      => \%counts,
        next_item   => next_queued_item($vault_root),
        items       => \@items,
    };
}

1;
