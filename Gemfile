source "https://rubygems.org"

gemspec

# CI runs the specs against several versions of the redis gem.
gem "redis", ENV["REDIS_GEM_VERSION"] if ENV["REDIS_GEM_VERSION"]

group :development, :test do
  gem "rake", "~> 13.0"
  gem "rspec", "~> 3.13"
  gem "rubocop", require: false
end
