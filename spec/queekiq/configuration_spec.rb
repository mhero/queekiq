require "spec_helper"

RSpec.describe Queekiq::Configuration do
  subject(:config) { described_class.new }

  describe "#redis_url" do
    it "defaults to REDIS_URL, then to localhost" do
      allow(ENV).to receive(:fetch).and_call_original
      allow(ENV).to receive(:fetch).with("REDIS_URL", described_class::DEFAULT_REDIS_URL).and_return("redis://from-env:6379/3")

      expect(config.redis_url).to eq("redis://from-env:6379/3")
    end

    it "can be assigned" do
      config.redis_url = "redis://elsewhere:6379/1"

      expect(config.redis.connection[:host]).to eq("elsewhere")
    end
  end

  describe "#redis" do
    it "memoizes the client it builds" do
      expect(config.redis).to equal(config.redis)
    end

    it "returns an assigned client untouched" do
      client = Redis.new
      config.redis = client

      expect(config.redis).to equal(client)
    end
  end

  describe "#instrumenter" do
    it "defaults to a built-in Instrumenter, memoized" do
      expect(config.instrumenter).to be_a(Queekiq::Instrumenter)
      expect(config.instrumenter).to equal(config.instrumenter)
    end

    it "can be replaced, e.g. by ActiveSupport::Notifications" do
      custom = double("instrumenter")
      config.instrumenter = custom

      expect(config.instrumenter).to equal(custom)
    end
  end

  describe "#logger" do
    it "returns an assigned logger" do
      logger = Logger.new(nil)
      config.logger = logger

      expect(config.logger).to equal(logger)
    end

    it "falls back to a stdout logger outside Rails" do
      expect(config.logger).to be_a(Logger)
    end

    it "uses Rails.logger when Rails is loaded and has one" do
      rails_logger = Logger.new(nil)
      stub_const("Rails", double("Rails", logger: rails_logger))

      expect(config.logger).to equal(rails_logger)
    end

    it "prefers an explicitly assigned logger over Rails.logger" do
      stub_const("Rails", double("Rails", logger: Logger.new(nil)))
      mine = Logger.new(nil)
      config.logger = mine

      expect(config.logger).to equal(mine)
    end
  end
end

RSpec.describe Queekiq do
  after { described_class.reset! }

  it "yields the global configuration" do
    described_class.configure { |c| c.redis_url = "redis://configured:6379/0" }

    expect(described_class.configuration.redis_url).to eq("redis://configured:6379/0")
  end

  it "exposes the configured redis client and logger" do
    expect(described_class.redis).to equal(described_class.configuration.redis)
    expect(described_class.logger).to equal(described_class.configuration.logger)
  end

  it "subscribes to events through the global instrumenter" do
    events = []
    described_class.subscribe("x.queekiq") { |name, _| events << name }

    described_class.instrumenter.instrument("x.queekiq")

    expect(events).to eq([ "x.queekiq" ])
  end

  it "drops the configuration on reset!" do
    original = described_class.configuration

    described_class.reset!

    expect(described_class.configuration).not_to equal(original)
  end
end
