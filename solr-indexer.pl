#!/usr/bin/perl

use warnings;
use strict;
use utf8;

use POE;
use POE::Wheel::Run;
use Data::Dumper;
use Sys::Hostname;

use File::Path;
use File::Basename;
use Getopt::Long;

our $DEBUG   = 1;
our $WORKERS = 50;

our $BASEDIR = "/var/spool/scribe";
our $WORKDIR = $BASEDIR."/processing";

if (! -d $WORKDIR) {
    mkpath($WORKDIR) || die("Could not create directory");
}

POE::Session->create(
    inline_states => {
        _start             => \&start_main,
        _stop              => \&stop_main,
        start_worker       => \&start_worker,
        worker_stdout      => \&worker_stdout,
        worker_stderr      => \&worker_stderr,
        worker_close       => \&worker_close,
        get_jobs           => \&get_jobs,
        report_status      => \&report_status,
        log_line           => \&log_line,
        sig_child          => \&sig_child,
        sig_hup            => \&sig_hup,
    },
    options => {
        trace => 0,
        debug => $DEBUG,
    }
);

POE::Kernel->run();

exit 0;

################################################################################

sub start_main {
    my ($kernel, $heap) = @_[KERNEL, HEAP];
    $kernel->yield("get_jobs");
    $kernel->yield("report_status");
    $kernel->yield("start_worker");
    $kernel->sig(HUP => 'sig_hup');
}

sub stop_main {

}

sub start_worker {
	my ($kernel, $heap) = @_[KERNEL, HEAP];
	my $curr_workers = keys(%{$heap->{workers}});
	my $num_jobs = @{$heap->{jobs}};

	if ( $curr_workers <= $WORKERS && $num_jobs > 0 ) {
		my $job = shift @{$heap->{jobs}};

		my $filename = basename($job);
		my $tmp = "$WORKDIR/$filename";
		rename($job, $tmp);

		my @program;
		push @program, '/usr/bin/python';
		push @program, '/usr/wikia/backend/bin/solr/page-worker.py';
		push @program, "--file=$tmp";

		my $worker = POE::Wheel::Run->new(
			#Program      => [ '/usr/bin/nice', '/usr/bin/perl', $prog, $arg, $tmp ],
			Program      => \@program,
			StdioFilter  => POE::Filter::Line->new(),
			StderrFilter => POE::Filter::Line->new(),
			StdoutEvent  => "worker_stdout",
			StderrEvent  => "worker_stderr",
			CloseEvent   => "worker_close",
		);

		$heap->{files}->{$worker->ID} = $tmp;
		$heap->{workers}->{$worker->ID} = $worker;
		$kernel->sig_child($worker->PID, "sig_child");

		$kernel->yield('log_line', "Indexer: Started worker PID ".$worker->PID." on $tmp");
	}
	$kernel->alarm("start_worker" => int(time()) + 1);
}

sub report_status {
	my ($kernel, $heap) = @_[KERNEL, HEAP];
	my $curr_workers = scalar keys %{$heap->{workers}};
	my $num_jobs = scalar @{$heap->{jobs}};

	# Report on works and jobs if there are any jobs waiting
	$kernel->yield('log_line', "Indexer: status - $curr_workers worker(s), $num_jobs job(s)");

	$kernel->alarm("report_status" => int(time()) + 10);
}

sub get_jobs {
	my ($kernel, $heap) = @_[KERNEL, HEAP];
	# Find all the files we're currently working on
	my %current = map { $_ => 1 } values %{$heap->{files}};

	# Process any files still in the WORKDIR left by dead workers
	my @orphaned = grep { not exists $current{$_} } get_files($WORKDIR);

	my @events   = get_files($BASEDIR."/events");
	my @retries  = get_files($BASEDIR."/retries");
	my @bulk     = get_files($BASEDIR."/bulk");
	my @failures = get_files($BASEDIR."/failures");
	@events = (@orphaned, @events, @retries, @bulk, @failures);

	$heap->{jobs} = \@events;
	$kernel->alarm("get_jobs" => int(time()) + 5);
}

sub worker_stdout {
	my ($kernel, $heap, $session, $stdout) = @_[KERNEL, HEAP, SESSION, ARG0];
	$heap->{updates}++ if $stdout =~ /^Updated:/;
	$heap->{deleted}++ if $stdout =~ /^Removed:/;
	if ($session->option('debug')) {
		$kernel->yield('log_line', "Worker: ".$stdout);
	}
}

sub worker_stderr {
	my ($kernel, $heap, $stderr) = @_[KERNEL, HEAP, ARG0];

	$heap->{trans_fail}++  if $stderr =~ /^Internal error/;
	$heap->{hard_fail}++   if $stderr =~ /^Failed/;
	$heap->{post_failed}++ if $stderr =~ /^Post failed/;

	$kernel->yield('log_line', "Worker ERROR: ".$stderr);
}

sub worker_close {
	my ($kernel, $heap, $worker_id) = @_[KERNEL, HEAP, ARG0];
	my $pid = $heap->{workers}->{$worker_id}->PID;

	$kernel->yield('log_line', "Indexer: Removing worker ${worker_id} with PID $pid");
	$heap->{workers_reaped}++;
	delete $heap->{workers}->{$worker_id};
	delete $heap->{files}->{$worker_id}
}

sub log_line {
	my ($kernel, $heap, $input) = @_[KERNEL, HEAP, ARG0];

	print STDERR gmtime(time()) . ": $input\n";
}

sub get_files {
	my ($path) = @_;
	
	opendir DIR, $path or die "Error reading $path: $!\n";
	my @files = sort {(-M "$path/$b")||0 <=> (-M "$path/$a")||0} readdir(DIR);
	closedir DIR;

	# If there is a 'current' symlink in this directory, don't include the
	# target of that symlink
	my ($curr_link) = grep { $_ =~ /_current$/ } @files;
	my $curr_file = $curr_link ? readlink("$path/$curr_link") : '';

	@files = grep { ( $_ =~ /\d{4}-\d{2}-\d{2}_\d{5}/ && $_ ne $curr_file ) || $_ =~ /\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}$/  } @files;
	@files = map { "$path/$_" } @files;

	my @non_zero;
	foreach my $f (@files) {
		if (-s $f) {
			push @non_zero, $f;
		} else {
			unlink($f);
		}
	}

	return @non_zero;
}

sub sig_child {
	print "Reaping worker\n";
}

sub sig_hup {
	my ($kernel, $heap, $session) = @_[KERNEL, HEAP, SESSION];

	$kernel->sig_handled();
	my $debug = $session->option('debug');
	print Dumper($debug);

	# Toggle the debug flag
	$session->option(debug => ($debug ? 0 : 1));
}

