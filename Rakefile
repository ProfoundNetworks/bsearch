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
task :selftest do
  sh "scripts/selftest"
end
