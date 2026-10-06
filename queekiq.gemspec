require_relative "lib/queekiq/version"

Gem::Specification.new do |spec|
  spec.name = "queekiq"
  spec.version = Queekiq::VERSION
  spec.authors = ["Marco Aviles"]
  spec.email = ["gdmarav374@gmail.com"]

  spec.summary = "Durable Redis Streams work queue with consumer groups, deferral and a worker loop."
  spec.description = "Queekiq is a small, framework-agnostic job queue built on Redis Streams: " \
                     "consumer groups for parallel workers, reclaiming of abandoned messages, " \
                     "delayed re-enqueueing (deferral) and a graceful consumer loop."
  spec.homepage = "https://github.com/mhero/queekiq"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.1"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["changelog_uri"] = "#{spec.homepage}/blob/main/CHANGELOG.md"
  spec.metadata["rubygems_mfa_required"] = "true"

  spec.files = Dir["lib/**/*.rb", "LICENSE.txt", "README.md", "CHANGELOG.md"]
  spec.require_paths = [ "lib" ]

  spec.add_dependency "redis", ">= 4.8", "< 7"
end
