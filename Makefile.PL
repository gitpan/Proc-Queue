use ExtUtils::MakeMaker;

WriteMakefile(
    'NAME'		=> 'Proc::Queue',
    'VERSION_FROM'	=> 'Queue.pm', # finds $VERSION
    'PREREQ_PM'		=> {}, # e.g., Module::Name => 1.1
);
