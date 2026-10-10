require "spec_helper"

RSpec.describe Queekiq::Instrumenter do
  subject(:instrumenter) { described_class.new }

  it "yields the payload and returns the block's value" do
    result = instrumenter.instrument("x.queekiq", a: 1) { |payload| payload[:a] + 1 }

    expect(result).to eq(2)
  end

  it "works without a block" do
    expect(instrumenter.instrument("x.queekiq")).to be_nil
  end

  it "notifies subscribers with the name and payload, adding the duration" do
    events = []
    instrumenter.subscribe { |name, payload| events << [ name, payload ] }

    instrumenter.instrument("x.queekiq", a: 1) { sleep 0.01 }

    name, payload = events.first
    expect(name).to eq("x.queekiq")
    expect(payload[:a]).to eq(1)
    expect(payload[:duration_ms]).to be >= 10
  end

  it "filters by exact name or regexp" do
    exact = []
    pattern = []
    instrumenter.subscribe("a.queekiq") { |name, _| exact << name }
    instrumenter.subscribe(/\Ab\./) { |name, _| pattern << name }

    %w[a.queekiq b.queekiq b.other c.queekiq].each { |name| instrumenter.instrument(name) }

    expect(exact).to eq([ "a.queekiq" ])
    expect(pattern).to eq(%w[b.queekiq b.other])
  end

  it "records the exception, still notifies, and re-raises" do
    events = []
    instrumenter.subscribe { |_, payload| events << payload }

    expect { instrumenter.instrument("x.queekiq") { raise "boom" } }.to raise_error("boom")

    expect(events.first[:exception]).to eq(%w[RuntimeError boom])
  end

  it "ignores subscribers that raise" do
    seen = []
    instrumenter.subscribe { |_, _| raise "broken subscriber" }
    instrumenter.subscribe { |name, _| seen << name }

    expect(instrumenter.instrument("x.queekiq") { :fine }).to eq(:fine)
    expect(seen).to eq([ "x.queekiq" ])
  end

  it "requires a block to subscribe" do
    expect { instrumenter.subscribe("x") }.to raise_error(ArgumentError)
  end
end
