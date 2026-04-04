package LlamaRun;
# LlamaRun — Call llama.cpp (CLI or HTTP server) with job-specific parameters.
# Supports: llama-cli (subprocess), llama-server (HTTP API), dry-run mode.

use strict;
use warnings;
use utf8;
use JSON::PP;
use HTTP::Tiny;
use File::Temp qw(tempfile);
use POSIX qw(strftime WNOHANG);
use IPC::Open3;
use IO::Select;
use Symbol qw(gensym);
use Encode qw(decode);
use Exporter 'import';

our @EXPORT_OK = qw(
    run_inference
    build_prompt
    load_config
    estimate_tokens
);

my $JSON = JSON::PP->new->utf8->pretty->canonical;

# ──────────────────────────────────────────────
# load_config($config_path) → hashref
#   Load the global nightly.json config (model path, server url, etc.)
# ──────────────────────────────────────────────
sub load_config {
    my ($config_path) = @_;
    die "Config not found: $config_path\n" unless -f $config_path;

    open my $fh, '<:raw', $config_path or die "Cannot open $config_path: $!\n";
    my $raw = do { local $/; <$fh> };
    close $fh;

    return $JSON->decode($raw);
}

# ──────────────────────────────────────────────
# build_prompt($profile, $context) → hashref
#   Assembles the full prompt from profile template + context bundle.
#   Returns: { system, user, output_contract }
# ──────────────────────────────────────────────
sub build_prompt {
    my ($profile, $context) = @_;
    my $prompt = $profile->{prompt};

    my $user_text = $prompt->{user_template};

    # Template substitution
    $user_text =~ s/\{\{frontmatter\}\}/$context->{frontmatter_yaml}/g;
    $user_text =~ s/\{\{body\}\}/$context->{body_text}/g;

    # Conditional: related notes
    my $related = $context->{related_notes_text} // '';
    if ($related =~ /\S/) {
        $user_text =~ s/\{\{#if related_notes\}\}(.*?)\{\{\/if\}\}/$1/gs;
        $user_text =~ s/\{\{related_notes\}\}/$related/g;
    } else {
        $user_text =~ s/\{\{#if related_notes\}\}.*?\{\{\/if\}\}//gs;
    }

    # Digest entries (for morning digest)
    if ($context->{digest_entries}) {
        $user_text =~ s/\{\{digest_entries\}\}/$context->{digest_entries}/g;
    }

    return {
        system          => $prompt->{system},
        user            => $user_text,
        output_contract => $prompt->{output_contract},
    };
}

# ──────────────────────────────────────────────
# run_inference($config, $profile, $prompt_parts) → string
#   Calls llama.cpp via the configured backend.
#   Returns the model's text output.
# ──────────────────────────────────────────────
sub run_inference {
    my ($config, $profile, $prompt_parts) = @_;

    my $backend = $config->{backend} || 'cli';
    my $dry_run = $config->{dry_run};

    # Build the full prompt text with role markers
    my $full_prompt = _assemble_chat_prompt(
        $config, $prompt_parts
    );

    if ($dry_run) {
        return _dry_run_output($profile, $full_prompt);
    }

    if ($backend eq 'server') {
        return _run_server($config, $profile, $prompt_parts);
    } else {
        return _run_cli($config, $profile, $full_prompt);
    }
}

# ── Backend: llama-cli subprocess ──

sub _run_cli {
    my ($config, $profile, $full_prompt) = @_;
    my $inf = $profile->{inference};

    my $llama_bin = $config->{llama_cli_path} || 'llama-completion';
    my $model     = $config->{model_path} or die "model_path required in config\n";

    # Write prompt to temp file to avoid shell escaping issues
    my ($tmp_fh, $tmp_path) = tempfile(SUFFIX => '.txt', UNLINK => 1);
    binmode($tmp_fh, ':encoding(UTF-8)');
    print $tmp_fh $full_prompt;
    close $tmp_fh;

    my @cmd = (
        $llama_bin,
        '-m', $model,
        '-f', $tmp_path,
        '-n', $inf->{num_predict}  || 1024,
        '--temp',    $inf->{temperature}    || 0.7,
        '--top-p',   $inf->{top_p}          || 0.9,
        '--top-k',   $inf->{top_k}          || 40,
        '--repeat-penalty', $inf->{repeat_penalty} || 1.1,
        '--repeat-last-n',  $inf->{repeat_last_n}  || 256,
        '--no-display-prompt',
        '--no-escape',
        '--stop', '[INST]',
    );

    # min_p support (newer llama.cpp)
    if ($inf->{min_p}) {
        push @cmd, '--min-p', $inf->{min_p};
    }

    # Mirostat sampling for prose/audio passes (overrides top-k/top-p)
    if ($inf->{mirostat}) {
        push @cmd, '--mirostat', $inf->{mirostat};
        push @cmd, '--mirostat-ent', $inf->{mirostat_ent} if $inf->{mirostat_ent};
        push @cmd, '--mirostat-lr',  $inf->{mirostat_lr}  if $inf->{mirostat_lr};
    }

    # Seed for reproducibility (optional)
    if (defined $inf->{seed}) {
        push @cmd, '-s', $inf->{seed};
    }

    # GPU layers — let Metal use all layers by default on Apple Silicon
    my $ngl = $config->{gpu_layers} // 99;
    push @cmd, '-ngl', $ngl;

    # Context size
    my $ctx = $config->{context_size} || 8192;
    push @cmd, '-c', $ctx;

    # Token budget validation: estimate prompt tokens + num_predict, warn if over context
    my $est_prompt_tokens = estimate_tokens($full_prompt);
    my $num_predict = $inf->{num_predict} || 1024;
    my $total_tokens = $est_prompt_tokens + $num_predict;
    my $safety_margin = 128;
    if ($total_tokens + $safety_margin > $ctx) {
        warn sprintf(
            "LlamaRun: token budget warning — est. prompt %d + predict %d = %d exceeds context %d (margin %d). Prompt may be silently truncated.\n",
            $est_prompt_tokens, $num_predict, $total_tokens, $ctx, $safety_margin
        );
    }

    my $cmd_str = join(' ', map { _shell_escape($_) } @cmd);
    my ($output, $stderr, $exit) = _run_command_with_timeout(
        \@cmd,
        timeout => ($config->{timeout} || 300),
    );

    if ($exit != 0) {
        # On timeout, return partial output if it looks usable (has ## headings)
        if ($exit == 124 && $output =~ /^##\s/m) {
            warn "LlamaRun: timed out but returning partial output (" . length($output) . " bytes)\n";
            return _clean_output($output);
        }
        my $detail = $stderr ? "\n$stderr" : '';
        die "llama-cli failed (exit $exit). Command: $cmd_str$detail\n";
    }

    return _clean_output($output);
}

# ── Backend: llama-server HTTP API ──

sub _run_server {
    my ($config, $profile, $prompt_parts) = @_;
    my $inf = $profile->{inference};

    my $server_url = $config->{server_url} || 'http://127.0.0.1:8080';

    # Build chat completion request
    my $messages = [
        { role => 'system', content => $prompt_parts->{system} },
        { role => 'user',   content => $prompt_parts->{user} . "\n\n" . $prompt_parts->{output_contract} },
    ];

    my $payload = {
        messages    => $messages,
        temperature => $inf->{temperature}    || 0.7,
        top_p       => $inf->{top_p}          || 0.9,
        n_predict   => $inf->{num_predict}    || 1024,
        repeat_penalty => $inf->{repeat_penalty} || 1.1,
        stream      => JSON::PP::false,
    };

    if ($inf->{min_p}) {
        $payload->{min_p} = $inf->{min_p};
    }

    my $http = HTTP::Tiny->new(timeout => $config->{timeout} || 300);
    my $resp = $http->post(
        "$server_url/v1/chat/completions",
        {
            content => $JSON->encode($payload),
            headers => { 'Content-Type' => 'application/json' },
        }
    );

    unless ($resp->{success}) {
        die "llama-server request failed: $resp->{status} $resp->{reason}\n$resp->{content}\n";
    }

    my $result = $JSON->decode($resp->{content});
    my $text = $result->{choices}[0]{message}{content}
        // die "No content in llama-server response\n";

    return _clean_output($text);
}

# ── Dry run: return placeholder output for testing ──

sub _dry_run_output {
    my ($profile, $full_prompt) = @_;
    my $name = $profile->{name} || 'unknown';
    my $date = strftime('%Y-%m-%d', localtime);

    my $contract = $profile->{prompt}{output_contract} || '';

    # Extract section headings from contract and generate stubs
    my @headings;
    while ($contract =~ /^(## .+)/gm) {
        push @headings, $1;
    }

    my $output = "";
    for my $h (@headings) {
        $output .= "$h\n[DRY RUN — $name pass — $date]\n\n";
    }

    $output .= "---\n*Prompt length: " . length($full_prompt) . " chars*\n";

    return $output;
}

# ── Build a chat-formatted prompt for CLI mode ──

sub _assemble_chat_prompt {
    my ($config, $prompt_parts) = @_;

    my $template = $config->{chat_template} || 'chatml';

    if ($template eq 'chatml') {
        return _chatml_format($prompt_parts);
    } elsif ($template eq 'llama3') {
        return _llama3_format($prompt_parts);
    } elsif ($template eq 'mistral') {
        return _mistral_format($prompt_parts);
    } else {
        # Default: simple concatenation
        return _simple_format($prompt_parts);
    }
}

sub _chatml_format {
    my ($p) = @_;
    return "<|im_start|>system\n$p->{system}<|im_end|>\n"
         . "<|im_start|>user\n$p->{user}\n\n$p->{output_contract}<|im_end|>\n"
         . "<|im_start|>assistant\n";
}

sub _llama3_format {
    my ($p) = @_;
    return "<|begin_of_text|><|start_header_id|>system<|end_header_id|>\n\n$p->{system}<|eot_id|>"
         . "<|start_header_id|>user<|end_header_id|>\n\n$p->{user}\n\n$p->{output_contract}<|eot_id|>"
         . "<|start_header_id|>assistant<|end_header_id|>\n\n";
}

sub _mistral_format {
    my ($p) = @_;
    # Mistral v0.2 instruct format: [INST] system\n\nuser\n\ncontract [/INST]
    # The GGUF model metadata has add_bos_token=true, so llama.cpp prepends <s>
    # (BOS token ID 1) automatically. A manual "<s>" in the text would tokenize
    # as literal angle-bracket-s-angle-bracket — 3 wasted tokens, not a BOS.
    # No trailing newline after [/INST] — model was trained to generate immediately.
    return "[INST] $p->{system}\n\n$p->{user}\n\n$p->{output_contract} [/INST]";
}

sub _simple_format {
    my ($p) = @_;
    return "System: $p->{system}\n\nUser: $p->{user}\n\n$p->{output_contract}\n\nAssistant:\n";
}

# ── Helpers ──

sub _run_command_with_timeout {
    my ($cmd, %opts) = @_;
    my $timeout = $opts{timeout} || 300;

    my $stderr_fh = gensym;
    my $stdout_fh;
    my $pid = open3(undef, $stdout_fh, $stderr_fh, @$cmd);

    my $selector = IO::Select->new();
    $selector->add($stdout_fh, $stderr_fh);

    my $stdout = '';
    my $stderr = '';
    my $timed_out = 0;

    eval {
        local $SIG{ALRM} = sub { die "__LLAMA_TIMEOUT__\n" };
        alarm $timeout;

        while ($selector->count) {
            my @ready = $selector->can_read(1);
            for my $fh (@ready) {
                my $buffer = '';
                my $bytes = sysread($fh, $buffer, 8192);
                next unless defined $bytes;

                if ($bytes == 0) {
                    $selector->remove($fh);
                    next;
                }

                if ($fh == $stdout_fh) {
                    $stdout .= $buffer;
                } else {
                    $stderr .= $buffer;
                }
            }
        }

        waitpid($pid, 0);
        alarm 0;
        1;
    } or do {
        my $error = $@ || 'unknown error';
        alarm 0;

        if ($error =~ /__LLAMA_TIMEOUT__/) {
            $timed_out = 1;
            _terminate_process($pid);
            $stderr .= "llama-cli timed out after ${timeout}s (partial output: " . length($stdout) . " bytes)";
        } else {
            _terminate_process($pid);
            die $error;
        }
    };

    my $exit = $? >> 8;
    if ($timed_out) {
        $exit ||= 124;
    }

    # Decode raw UTF-8 bytes from subprocess into Perl Unicode strings
    $stdout = decode('UTF-8', $stdout);
    $stderr = decode('UTF-8', $stderr);

    return ($stdout, $stderr, $exit);
}

sub _terminate_process {
    my ($pid) = @_;
    return unless $pid;

    kill 'TERM', $pid;
    for (1..5) {
        my $done = waitpid($pid, WNOHANG);
        return if $done == $pid;
        select undef, undef, undef, 0.2;
    }

    kill 'KILL', $pid;
    waitpid($pid, 0);
}

sub _clean_output {
    my ($text) = @_;

    # Strip llama.cpp loading/build noise lines first
    $text =~ s/^.*Loading model\..*$//gm;
    $text =~ s/^[\x{2500}-\x{257F}\x{2580}-\x{259F}\x{2588}\x{2591}-\x{2593}\x{25A0}-\x{25FF}\s]+$//gm;
    $text =~ s/^build\s+:.*$//gm;
    $text =~ s/^model\s+:.*$//gm;
    $text =~ s/^modalities\s+:.*$//gm;
    $text =~ s/^available commands:.*?(?=\n\n)/\n/s;
    $text =~ s/^>\s*\[INST\].*?\[\/?INST\]\s*//s;

    # Try to find structured output starting with ## heading
    if ($text =~ /^(\s*##.+)/ms) {
        $text =~ s/^.*?(?=^\s*##)//ms;
    }
    # For audio/prose passes that don't use ## headings, strip everything
    # before the first real paragraph (3+ words on a line)
    elsif ($text =~ /^(.{0,500}?)((?:[A-Z][a-z]+ ){2,}.+)/s) {
        $text = $2;
    }

    # Strip performance stats footer
    $text =~ s/\[\s*Prompt:.*?Generation:.*?\]//g;

    # Strip "Exiting..." line
    $text =~ s/^Exiting\.\.\.\s*$//gm;

    # Strip EOF markers from llama-completion
    $text =~ s/^>\s*EOF.*$//gm;
    $text =~ s/^>\s*$//gm;

    # Strip llama-cli/llama-completion warnings
    $text =~ s/^--no-conversation.*$//gm;
    $text =~ s/^please use llama-completion.*$//gm;

    # Strip stray chat template tokens
    $text =~ s/<\|im_end\|>//g;
    $text =~ s/<\|im_start\|>//g;
    $text =~ s/<\|eot_id\|>//g;
    $text =~ s/<\|end_of_text\|>//g;
    $text =~ s/\[\/?INST\]//g;
    $text =~ s/<\/s>//g;

    # Fix malformed wiki-links: [[Name|[Name][N]]] → [[Name]]
    $text =~ s/\[\[([^\]|]+)\|\[[^\]]+\]\[\d+\]\]\]/[[$1]]/g;
    # Fix reference-style links model sometimes generates: [Name][1] → [[Name]]
    $text =~ s/\[([^\]]+)\]\[\d+\]/[[$1]]/g;

    # Strip leading/trailing whitespace and collapse blank lines
    $text =~ s/\A\s+//;
    $text =~ s/\s+\z//;
    $text =~ s/\n{3,}/\n\n/g;

    return $text;
}

sub _shell_escape {
    my ($arg) = @_;
    $arg =~ s/'/'\\''/g;
    return "'$arg'";
}

# ──────────────────────────────────────────────
# estimate_tokens($text) → int
#   Rough token count for Mistral's SentencePiece tokenizer.
#   English prose: ~3.5 chars/token. Markdown/YAML/code: ~2.8 chars/token.
#   We use 3.2 as a conservative blended estimate (errs on the high side).
# ──────────────────────────────────────────────
sub estimate_tokens {
    my ($text) = @_;
    return 0 unless defined $text && length($text);
    use POSIX qw(ceil);
    return ceil(length($text) / 3.2);
}

1;
