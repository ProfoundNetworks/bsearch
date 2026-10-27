
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
  # Dup-key datasets are tested on their full, sorted contents: selftest's
  # dup-key path groups consecutive input lines by key, so it needs sorted
  # input and can't take the shuffled _r1 files (a sorted sample would do)
  dupkey = [ "data/foo.csv" ]
  dupkey.each do |ds|
    puts ds.colorize(name_colour).bold
    sh "time -f 'Elapsed: %E' cat #{ds} | cmd/bsearch_selftest/bsearch_selftest --hdr -i #{ds} | ctap -gsf", :verbose => false
    puts
  end

  # Uniq-key datasets check each line independently, so we can test them
  # with shuffled _r1 inputs instead
  FileList['data/*_r1.[cpt]sv'].each do |rand|
    ds = rand.sub('_r1.', '.')
    puts ds.colorize(name_colour).bold
    sh "time -f 'Elapsed: %E' pv -l #{rand} | cmd/bsearch_selftest/bsearch_selftest --hdr -i #{ds} | ctap -gsf", :verbose => false
    puts
  end
end
