require "securerandom"
require "logger"
require "queekiq"

# The specs talk to a real Redis (>= 7.0). Point REDIS_URL at it if it isn't
# on localhost:6379; each example uses its own uniquely named queue.
RSpec.configure do |config|
  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end

  config.mock_with :rspec do |mocks|
    mocks.verify_partial_doubles = true
  end

  config.shared_context_metadata_behavior = :apply_to_host_groups

  config.before do
    Queekiq.configuration.logger = Logger.new(nil)
    Queekiq.configuration.instrumenter = Queekiq::Instrumenter.new
  end
end
