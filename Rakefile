# Bold blue, without needing the colorize gem
def heading(s)
  "\e[1;34m#{s}\e[0m"
end

task :default => :test

desc "Run tests"
task :test do
  sh "richgo test -v ./..."
end

desc "Build the binaries listed in scripts/push.list into bin/"
task :build do
  sh "scripts/build_all"
end

# There is no default host (see scripts/push), so name one or more:
#   rake push[host1,host2]
#   PUSH_HOST=host1,host2 rake push
desc "Push binaries to the named hosts (rake push[HOST,...] or PUSH_HOST)"
task :push do |_t, args|
  if args.extras.empty?
    sh "scripts/push"
  else
    sh "scripts/push", args.extras.join(",")
  end
end

desc "Run selftests against the data/ datasets (slow)"
task :selftest => :build do
  # bsearch_selftest isn't pushed, so build_all doesn't build it
  sh "go build -buildvcs=false -o bin/bsearch_selftest ./cmd/bsearch_selftest"

  # Dup-key datasets are tested on their full, sorted contents: selftest's
  # dup-key path groups consecutive input lines by key, so it needs sorted
  # input and can't take the shuffled _r1 files (a sorted sample would do)
  dupkey = [ "data/foo.csv" ]
  dupkey.each do |ds|
    puts heading(ds)
    sh "bin/bsearch_index #{ds}", :verbose => false
    sh "time -f 'Elapsed: %E' cat #{ds} | bin/bsearch_selftest --hdr -i #{ds} | ctap -gsf", :verbose => false
    puts
  end

  # Uniq-key datasets check each line independently, so we can test them
  # with shuffled _r1 inputs instead
  FileList['data/*_r1.[cpt]sv'].each do |rand|
    ds = rand.sub('_r1.', '.')
    puts heading(ds)
    sh "bin/bsearch_index #{ds}", :verbose => false
    sh "time -f 'Elapsed: %E' pv -l #{rand} | bin/bsearch_selftest --hdr -i #{ds} | ctap -gsf", :verbose => false
    puts
  end
end
