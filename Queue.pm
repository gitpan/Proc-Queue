package Proc::Queue;

require 5.005;

our $VERSION = '1.15';

use strict;
# use warnings;
require Exporter;
use Carp;
use POSIX ":sys_wait_h";

our @ISA = qw(Exporter);
our %EXPORT_TAGS = ( all => [ qw( fork_now
				  waitpids
				  run_back
				  run_back_now
				  system_back
				  system_back_now
				  all_exit_ok
				  running_now ) ] );
our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );
our @EXPORT = qw();

# are we running on Windows?
use constant _win => $^O=~/win32/i;

# parameters
my $queue_size=4; # max number of councurrent processes running.
my $debug=0; # shows debug info.
my $trace=0; # shows function calls.
my $delay=0; # delay between fork calls.
my $ignore_childs=0; # similar to $SIG{CHILD}='IGNORE';

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
  for ($i=0; $i<@opts; $i++) {
    my $o=$opts[$i];
    if( $o eq 'size'
        or $o eq 'debug'
        or $o eq 'trace'
	or $o eq 'delay'
	or $o eq 'ignore_childs') {
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
  return $delay unless @_;
  my $old_delay=$delay;
  $delay=$_[0];
  carp "Proc queue delay set to ${delay}s, it was $old_delay" if $debug;
  return $old_delay;
}

sub size {
  return $queue_size unless @_;
  my $size=shift;
  my $old_size=$queue_size;
  croak "invalid value for Proc::Queue size ($size), min value is 1"
    unless $size >= 1;
  $queue_size=$size;
  carp "Proc queue size set to $size, it was $old_size" if $debug;
  return $old_size;
}

sub debug {
  return $debug unless @_;
  my $old_debug=$debug;
  if ($_[0]) {
    $debug=1;
    carp "debug mode ON";
  }
  else {
    $debug=0;
    carp "debug mode OFF" if $old_debug;
  }
  return $old_debug;
}

sub trace {
  return $trace unless @_;
  my $old_trace=$trace;
  if ($_[0]) {
    $trace=1;
    carp "trace mode ON" if $debug;
  }
  else {
    $trace=0;
    carp "trace mode OFF" if $debug;
  }
  return $old_trace;
}

sub ignore_childs {
  return $ignore_childs unless @_;
  my $old_ignore_childs=$ignore_childs;
  if ($_[0]) {
    $ignore_childs=1;
    carp "ignore_childs mode ON" if $debug;
  }
  else {
    $ignore_childs=0;
    carp "ignore_childs mode OFF" if $debug;
  }
  return $old_ignore_childs;
}

# sub to store internally captured processes
sub _push_captured {
  push @captured, shift,$? unless $ignore_childs;
  croak "captured stack is corrupted" if (@captured & 1)
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
    carp "Wait returning old child $w captured in wait" if $debug;
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
  while (1) {
    $pid=_waitpid(-1,WNOHANG);
    return unless ((_win && $pid < -1) || $pid >0);
    _push_captured $pid
  }
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
  my $e=@_?shift:0;
  carp "Proc::Queue::exit(".(defined($e)?$e:'undef').") called" if $trace;
  carp "Process $$ exiting with value ".(defined($e)?$e:'undef') if $debug;
  return CORE::exit($e);
}

# use Time::Hires::time if available;
BEGIN { eval "use Time::HiRes 'time'" }

sub _fork () {
  carp "Proc::Queue::_fork called" if $trace && $debug;
  if ($delay>0) {
    my $wait=$last+$delay - time;
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
    carp "Waiting for some process to finish" if $debug;
    my $nw;
    if (($nw=_wait) != -1) {
      _push_captured $nw;
    }
    else {
      carp "Proc queue seems to be corrupted, $queue_now childs lost";
      last;
    }
  }
  return _fork;
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
      if ((_win && $r < -1) || $r > 0) {
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

sub _system_back {
  my $now=shift;
  carp "Proc::Queue::_system_back($now, ".join(", ",@_).") called" if $trace and $debug;
  my $f=$now ? fork_now : new_fork;
  if(defined($f) and $f==0) {
    carp "Running exec(".join(", ",@_).") in forked child $$" if $debug;
    { exec(@_) }
    carp "exec(".join(", ",@_).") failed" if $debug;
  }
  return $f;
}

sub system_back {
  carp "Proc::Queue::system_back(".join(", ",@_).") called" if $trace;
  return _system_back(0,@_);
}

sub system_back_now {
  carp "Proc::Queue::system_back_now(".join(", ",@_).") called" if $trace;
  return _system_back(1,@_);
}

sub all_exit_ok {
  carp "Proc::Queue::all_exit_ok(".join(", ",@_).")" if $trace;
  my @result=waitpids(@_);
  my $i;
  for($i=0;$i<@result;$i++) {
    next unless $i&1;
    if (!defined($result[$i]) or $result[$i]) {
      carp "Child ".$_[$i>>1]." fail with code $result[$i], waitpid return $result[$i-1]" if $debug;
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

Proc::Queue - limit the number of child processes running

=head1 SYNOPSIS

  use Proc::Queue size => 4, debug => 1;

  package other;
  use POSIX ":sys_wait_h"; # imports WNOHANG

  # this loop creates new childs, but Proc::Queue makes it wait every
  # time the limit (4) is reached until enough childs exit
  foreach (1..100) {
    my $f=fork;
    if(defined ($f) and $f==0) {
      print "-- I'm a forked process $$\n";
      sleep rand 5;
      print "-- I'm tired, going away $$\n";
      exit(0)
    }
    1 while waitpid(-1, WNOHANG)>0; # reaps childs
  }

  Proc::Queue::size(10); # changing limit to 10 concurrent processes
  Proc::Queue::trace(1); # trace mode on
  Proc::Queue::debug(0); # debug is off
  Proc::Queue::delay(0.2); # set 200 miliseconds as minimum
                           # delay between fork calls

  package other; # just to test it works on any package

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

This module lets you parallelise a perl program using the C<fork>,
C<exit>, C<wait> and C<waitpid> calls as usual but without taking care
of creating too many processes and overloading the machine.

It redefines perl C<fork>, C<exit>, C<wait> and C<waitpid> core
functions. Old programs do not need to be reprogrammed, only the C<use
Proc::Queue ...> sentence has to be added to them.

Additionally, the module has two debugging modes (debug and trace)
that seem too be very useful when developing parallel aplications:

=over 4

=item debug mode:

when active, dumps lots of information about processes being created,
exiting, being caught by parent, etc.

=item trace mode:

prints a line every time one of the C<fork>, C<exit>, C<wait> or
C<waitpid> functions are called.

=back

It is also possible to set a minimun delay time between fork calls
to stop too many processes for starting in a short time interval.

Child processes continue to use the modified functions, but their
queues are reset and the maximun process number for them is set to 1
(anyway, childs can change their queue size themselves).

Proc::Queue doesn't work if CHLD signal handler is set to
C<IGNORE>.  

Internally, Proc::Queue, automatically catches zombies and stores
their exit status in a private hash. To avoid leaking memory in long
running programs you have to call C<wait> or C<waitpid> to delete
entries from that hash or alternatively active the C<ignore_childs>
mode:

  Proc::Queue::ignore_childs(1)

or

  use Proc::Queue ignore_childs=>1, ...


=head2 EXPORT

This module redefines the C<fork>, C<wait>, C<waitpid> and C<exit>
calls.

=head2 EXPORT_OK

Functions C<fork_now>, C<waitpids>, C<run_back>, C<run_back_now>,
C<all_exit_ok>, C<running_now>, C<system_back> and C<system_back_now>
can be imported. Tag C<:all> is defined to import all of them.

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

lets you set a minimun time in seconds to elapse between consecutive
calls to fork. It is useful to avoid creating too many processes in
a short time (that could degrade performance).

If Time::HiRes module is available delays shorted that 1 second are
allowed.

If no arg is given, the current delay is returned.

To clear it use C<Proc::Queue::delay(0)>.

=item ignore_childs($on)

calling

  Proc::Queue::ignore_childs(1);

is the equivalent to

  $SIG{CHLD}='IGNORE'

when using Proc::Queue.

=item debug(), debug($boolean), trace(), trace($boolean)

Change or return the status for the debug and trace modes.

=back


Other utility subroutines that can be imported from Proc::Queue are:


=over 4

=item fork_now()

Sometimes you would need to fork a new child without waiting for other
childs to exit if the queue is full, C<fork_now> does that. It is
exportable so you can do...

  use Proc::Queue size => 5, qw(fork_now), debug =>1;

  $f=fork_now;
  if(defined $f and $f == 0) {
      print "I'm the child\n"; exit;
  }

=item waitpids(@pid)

waits for all the processes in @pid to exit. It returns an array
with pairs of pid and exit values (pid1, exit1, pid2, exit2, pid3,
exit3,...) as returned by individual waitpid calls.

=item run_back(\&code), run_back { code }

Runs the argument subrutine in a forked child process and returns the
pid number for the new process.

=item run_back_now(\&code), run_back_now { code }

A mix between run_back and fork_now.

=item system_back(@command)

Similar to the C<system> call but runs the command in the background
and waits for other childs to exit first if there are already too many
running.

=item system_back_now(@command)

As C<system_back> but without checking if the maximun number of childs
allowed has been reached.

=item all_exit_ok(@pid)

Do a C<waitpids> call and test that all the processes exit with code 0.

=item running_now()

Returns the number of child processes currently running.

=item import(pkg,opt,val,opt,val,...,fnt_name,fnt_name,...)

The import function is not usually explicitally called but by the
C<use Proc::Queue> statement.

Options allowed are C<size>, C<debug> and C<trace>, i.e:

  use Proc::Queue size=>10, debug=>1;

Anything that is not C<size>, C<debug> or C<trace> is expected to be a
function name to be imported.

  use Proc::Queue size=>10, ':all';


=head2 BUGS

Proc::Queue is a very stable module, no bugs have been reported
for a long time.

Support for Win32 OSs is still experimental.

=head1 SEE ALSO

L<perlfunc(1)>, L<perlipc(1)>, L<POSIX>, L<perlfork(1)>,
L<Time::HiRes>, L<Parallel::ForkManager>. The C<example.pl> script
contained in the module distribution.


=head1 AUTHOR

Salvador Fandiño E<lt>sfandino@yahoo.comE<gt>


=head1 COPYRIGHT AND LICENSE

Copyright 2001, 2002, 2003 by Salvador Fandiño

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
