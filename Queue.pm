package Proc::Queue;

require 5.005_62;
use strict;
use warnings;
require Exporter;
use Carp;

use POSIX ":sys_wait_h";

our @ISA = qw(Exporter);

our %EXPORT_TAGS = ( all => [ qw( fork_now
				  waitpids
				  run_back
				  run_back_now
				  all_exit_ok
				  running_now ) ] );

our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );
our @EXPORT = qw();

our $VERSION = '0.08';

# parameters
my $queue_size=4; # max number of councurrent processes running.
my $debug=0; # shows debug info.
my $trace=0; # shows function enters.
my $delay=0; # delay between fork calls.

my $last=0; # last time fork was called.

# module status
my $queue_now=0;
my %process;
my @captured;

# set STDERR as unbuffered so all the carp calls work as expected
{ my $oldfh=select STDERR; $|=1; select $oldfh }

# extended import to support parameter configuration from use statment
sub import {
  my ($pkg,@opts)=@_;
  my $i;
  for ($i=0; $i<=$#opts; $i++) {
    my $o=$opts[$i];
    if( $o eq 'size'
        or $o eq 'debug'
        or $o eq 'trace'
	or $o eq 'delay' ) {
      $#opts>$i or croak "option '$o' needs a value";
      my $value=$opts[$i+1];
      { no strict qw( subs refs );
	&$o($value) }
      splice @opts,$i--,2;
    }
  }
  carp "Exporting '".join("', '",@opts)."' symbols from Proc::Queue" if $debug;
  @_=($pkg,@opts);
  goto &Exporter::import;
}

sub delay {
  my $d=shift;
  my $old_delay=$delay;
  if(defined $d) {
    $delay=$d;
    carp "Proc queue delay set to $delay sec., it was $old_delay" if $debug;
  }
  return $old_delay;
}

sub size {
  my $size=shift;
  my $old_size=$queue_size;
  if(defined $size) {
    croak "invalid value for Proc::Queue size ($size), min value is 1"
      unless $size >= 1;
    $queue_size=$size;
    carp "Proc queue size set to $queue_size, it was $old_size" if $debug;
  }
  return $old_size;
}

sub debug {
  my $d=shift;
  my $old_debug=$debug;
  if(defined $d) {
    if ($d) {
      $debug=1;
      carp "Debug mode is now on for Proc::Queue module";
    }
    else {
      $debug=0;
      carp "Debug mode is now off for Proc::Queue module" if $old_debug;
    }
  }
  return $old_debug;
}

sub trace {
  my $t=shift;
  my $old_trace=$trace;
  if(defined $t) {
    if ($t) {
      $trace=1;
      carp "Trace mode is now on for Proc::Queue module" if $debug;
    }
    else {
      $trace=0;
      carp "Trace mode is now off for Proc::Queue module" if $debug;
    }
  }
  return $old_trace;
}

# sub to store internally captured processes
sub _push_captured {
  push @captured, shift,$?;
  croak "captured stack is corrupted" unless ($#captured & 1)
}

# do the real wait and housekeeping
sub _wait () {
  carp "Proc::Queue::_wait private function called" if $debug && $trace;
  carp "Waiting for child processes to exit" if $debug;
  my $w=CORE::wait;
  if ($w != -1) {
    if(exists $process{$w}) {
      delete $process{$w};
      $queue_now--;
      carp "Process $w has exited, $queue_now processes running now" if $debug;
    }
    else {
      carp "Unknow process $w has exited, ignoring it" if $debug;
    }
  }
  else {
    carp "No child processes left, continuing" if $debug;
  }
  return $w;
}

sub new_wait () {
  carp "Proc::Queue::wait called" if $trace;
  if(@captured) {
    my $w=shift @captured;
    $?=shift @captured;
    carp "Wait returning old child $w captured in fork" if $debug;
    return $w;
  }
  return _wait;
}

sub _waitpid ($$) {
  my ($pid,$flags)=@_;
  carp "Proc::Queue::_waitpid($pid,$flags) private function called" if $debug && $trace;
  carp "Waiting for child process $pid to exit" if $debug;
  my $w=CORE::waitpid($pid,$flags);
  if ($w != -1) {
    if(exists $process{$w}) {
      delete $process{$w};
      $queue_now--;
      carp "Process $w has exited, $queue_now processes running now" if $debug;
    }
    else {
      carp "Unknow process $w has exited, ignoring it" if $debug;
    }
  }
  else {
    carp "No child processes left, continuing" if $debug;
  }
  return $w;
}

sub _clean() {
  my $pid;
  _push_captured($pid) while(($pid=_waitpid(-1,WNOHANG))>0);
}

sub new_waitpid ($$) {
  my ($pid,$flags)=@_;
  carp "Proc::Queue::waitpid called" if $trace;
  foreach my $i (0..$#captured) {
    next if $i&1;
    my $r;
    if ($pid==$captured[$i] or $pid==-1) {
      croak "corrupted captured stack" unless ($#captured & 1);
      ($r,$?)=splice @captured,$i,2;
      return $r;
    }
  }
  return _waitpid($pid,$flags);
}

sub new_exit (;$ ) {
  my $e=shift;
  carp "Proc::Queue::exit($e) called" if $trace;
  carp "Process $$ exiting with value $e" if $debug;
  return CORE::exit($e);
}


# use Time::Hires::time if available;
BEGIN { eval "use Time::HiRes 'time'" }

sub _fork () {
  carp "Proc::Queue::_fork called" if $trace && $debug;
  if ($delay>0) {
    my $wait=$last+$delay - time();
    if ($wait>0) {
      carp "Delaying $wait seconds before forking" if $debug;
      select (undef,undef,undef,$wait);
    }
    $last=time;
  }
  my $f=CORE::fork;
  if (defined($f)) {
    if($f == 0) {
      carp "Process $$ now running" if $debug;
      # reset queue internal vars in child proccess;
      $queue_size=1;
      $queue_now=0;
      %process=();
      @captured=();
    }
    else {
      $process{$f}=1;
      $queue_now++;
      carp "Child forked (pid=$f), $queue_now processes running now" if $debug;
    }
  }
  else {
    carp "Fork failed: $!" if $debug;
  }
  return $f;
}

sub new_fork () {
  carp "Proc::Queue::fork called" if $trace;
  while($queue_now>=$queue_size) {
    carp "Waiting that some process finishes before continuing" if $debug;
    my $nw;
    if (($nw=_wait) != -1) {
      _push_captured $nw;
    }
    else {
      carp "Proc queue seems to be corrupted, $queue_now childs lost";
      last;
    }
  }
  return _fork();
}

sub fork_now () {
  carp "Proc::Queue::fork_now called" if $trace;
  return _fork;
}

sub waitpids {
  carp "Proc::Queue::waitpids(".join(", ",@_).")" if $trace;
  my @result;
  foreach my $pid (@_) {
    if (defined($pid)) {
      carp "Waiting for child $pid to exit" if $debug;
      my $r=new_waitpid($pid,0);
      if ($r > 0) {
	carp "Child $r return $?" if $debug;
	push @result,$r,$?;
      }
      else {
	carp "No such child returned while waiting for $pid" if $debug;
	push @result,$r,undef;
      }
    }
    else {
      carp "Undef arg found";
      push @result,undef,undef
    };
  }
  return @result;
}


sub _run_back {
  my ($now,$code)=@_;
  carp "Proc::Queue::_run_back($now,$code) called" if $trace and $debug;
  my $f=$now ? fork_now : new_fork;
  if(defined($f) and $f==0) {
    carp "Running code $code in forked child $$" if $debug;
    $?=0;
    eval {
      &$code();
    };
    if ($@) {
      carp "Uncaught exception $@" if $debug;
      new_exit(255)
    }
    else {
      carp "Code $code in child $$ returns '$?'" if $? && $debug;
    }
    new_exit($?)
  }
  return $f;
}

sub run_back (&) {
  my $code=shift;
  carp "Proc::Queue::run_back($code) called" if $trace;
  return _run_back(0,$code)
}

sub run_back_now (&) {
  my $code=shift;
  carp "Proc::Queue::run_back_now($code) called" if $trace;
  return _run_back(1,$code)
}

sub all_exit_ok {
  carp "Proc::Queue::all_exit_ok(".join(", ",@_).")" if $trace;
  my @result=waitpids(@_);
  my $i;
  for($i=0;$i<=$#result;$i++) {
    next unless $i&1;
    if (!defined($result[$i]) or $result[$i]) {
      carp "Child $_[$i>>1] fail with code $result[$i], waitpid return $result[$i-1]" if $debug;
      return 0;
    }
  }
  carp "All childs run ok" if $debug;
  return 1;
}

# this function is mostly for testing pourposes:
sub running_now () {
  _clean;
  return $queue_now;
}

*CORE::GLOBAL::wait = \&new_wait;
*CORE::GLOBAL::waitpid = \&new_waitpid;
*CORE::GLOBAL::exit = \&new_exit;
*CORE::GLOBAL::fork = \&new_fork;


1;
__END__

# docs:

=head1 NAME

Proc::Queue - Perl extension to limit the number of concurrent child
process running

=head1 SYNOPSIS

  use Proc::Queue size => 4, debug => 1;

  package other;

  # this loop will create new childs, but Proc::Queue will make it
  # wait when the limit (4) is reached until some of the old childs
  # exit.
  foreach (1..10) {
    my $f=fork;
    if(defined ($f) and $f==0) {
      print "-- I'm a forked process $$\n";
      sleep rand 5;
      print "-- I'm tired, going away $$\n";
      exit(0)
    }
  }

  Proc::Queue::size(10); # changing limit to 10 concurrent processes
  Proc::Queue::trace(1); # trace mode on
  Proc::Queue::debug(0); # debug is off
  Proc::Queue::delay(0.2); # set 200 miliseconds as minimum
                           # delay between fork calls

  package other; # just to test it works in any package

  print "going again!\n";

  # another loop with different settings for Proc::Queue
  foreach (1..20) {
    my $f=fork;
    if(defined ($f) and $f==0) {
      print "-- I'm a forked process $$\n";
      sleep rand 5;
      print "-- I'm tired, going away $$\n";
      exit(0)
    }
  }

  1 while wait != -1;

=head1 DESCRIPTION

This module lets you parallelice one program using the C<fork>,
C<exit>, C<wait> and C<waitpid> calls as usual and without the need to
take care of creating too much processes and overloading the machine.

It works redefining C<fork>, C<exit>, C<wait> and C<waitpid> functions
so old programs do not have to be modified to use this module (only
the C<use Proc::Queue> sentence is needed).

Additionally, the module have two debugging modes (debug and trace)
that can be activated and that seem too be very useful when developing
parallel aplications.

Debug mode when activated dumps lots of information about processes
being created, exiting, being caught be parent, etc.

Trace mode just prints a line every time one of the C<fork>, C<exit>,
C<wait> or C<waitpid> functions is called.

It is also possible to set a minimun delay time between calls to fork
to stop consecutive processes for starting in a short time interval.

Childs processes continue to use the modified functions, but its
queues are reset and the maximun process number for them is set to
1. Althought childs can change it to any other value if needed.

=head2 EXPORT

This module redefines the C<fork>, C<wait>, C<waitpid> and C<exit>
calls.

=head2 EXPORT_OK

Functions C<fork_now>, C<waitpids>, C<run_back>, C<run_back_now>,
C<all_exit_ok> and C<running_now> can be imported. Tag C<:all> is
defined to import all of them.

=head2 FUNCTIONS

There are several not exported functions that can be used to configure
the module:

=over 4

=item size(),  size($number)

If an argument is given the maximun number of concurrent processes is
set to it and the number of maximun processes that were allowed before
is returned.

If no argument is given, the number of processes allowed is returned.

=item delay(), delay($time)

delay lets you set a minimun time in seconds to elapse between every
consecutive calls to fork. This is usefull for not creating to much
processes in a very short time.

If the Time::HiRes module is available it will be used and delays
shorted that 1 second could be used.

If no arg is given, the current delay is returned.

To clear it use C<Proc::Queue::delay(0)>.

=item fork_now()

Sometimes you would need to fork a new child without waiting for other
child to exit if the queue is full, C<fork_now> does that. It is
exportable so you can do...

  use Proc::Queue size => 5, qw(fork_now), debug =>1;

  $f=fork_now;
  if(defined $f and $f == 0) {
      print "I'm the child\n"; exit;
  }

=item waitpids(@pid)

Will wait for all the processes in @pid to exit. It returns an array
with pairs pid and exit values (pid1, exit1, pid2, exit2, pid3,
exit3,...) as returned by individual waitpid calls.

=item run_back(\&code), run_back { code }

Runs the argument subrutine in a forked child process and returns the
pid number for the new process.

=item run_back_now(\&code), run_back_now { code }

A mix between run_back and fork_now.

=item all_exit_ok(@pid)

Do a waitpids call and test that all the processes exit with code 0.

=item running_now()

Returns the number of child processes currently running.

=item debug(), debug($boolean), trace(), trace($boolean)

Change or return the status for the debug and trace modes.

=item import(pkg,opt,val,opt,val,...,fnt_name,fnt_name,...)

The import function is not usually explicitally called but by the
C<use Proc::Queue> statement. The options allowed are C<size>, C<debug>
and C<trace> and they let you configure the module instead of using
the C<size>, C<debug> or C<trace> module functions as in...

  use Proc::Queue size=>10, debug=>1;

Anything that is not C<size>, C<debug> or C<trace> is expected to be a
function name to be imported.

  use Proc::Queue size=>10, ':all';

=head2 BUGS

None that I know, but this is just version 0.08!

The module has only been tested under Solaris 2.6

Child (forking) behaviour althought deterministic could be changed to
something better. I would accept any suggestions on it.

=head1 INSTALL

As usual, unpack de module distribution and from the newly created
directory run:

  $ perl Makefile.PL
  $ make
  $ make test
  $ make install

=head1 AUTHOR

Salvador Fandino <sfandino@yahoo.com>

=head1 SEE ALSO

L<perlfunc(1)>, L<perlipc(1)>, L<POSIX>, L<perlfork(1)>,
L<Time::HiRes>, L<Parallel::ForkManager>. The C<example.pl> script
contained in the module distribution.

=cut
