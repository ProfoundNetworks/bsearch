
require 'colorize'
section_colour = :yellow
name_colour = :blue

task :default => :test

desc "Run tests"
task :test do
  sh "richgo test -v ./..."
end

desc "Run selftests (slow)"
task :selftest do
  # For dup-key datasets we can't run randomised inputs
  dupkey = [ "data/foo.csv" ]
  dupkey.each do |ds|
    puts ds.colorize(name_colour).bold
    sh "time -f 'Elapsed: %E' cat #{ds} | cmd/bsearch_selftest/bsearch_selftest --hdr -i #{ds} | ctap -gsf", :verbose => false
    puts
  end

  # For uniq-key datasets we run randomised inputs instead
  FileList['data/*_r1.[cpt]sv'].each do |rand|
    ds = rand.sub('_r1.', '.')
    puts ds.colorize(name_colour).bold
    sh "time -f 'Elapsed: %E' pv -l #{rand} | cmd/bsearch_selftest/bsearch_selftest --hdr -i #{ds} | ctap -gsf", :verbose => false
    puts
  end
end
