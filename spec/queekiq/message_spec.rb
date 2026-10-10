require "spec_helper"

RSpec.describe Queekiq::Message do
  it "needs a payload or a raw value" do
    expect { described_class.new(id: "1-0") }.to raise_error(ArgumentError, /payload: or raw:/)
  end

  it "defaults to a first delivery with no history" do
    message = described_class.new(id: "1-0", payload: "10")

    expect(message.payload).to eq("10")
    expect(message.raw).to eq("10")
    expect([ message.attempt, message.defers, message.enqueued_at ]).to eq([ 1, 0, nil ])
  end

  it "serializes a given payload to get the raw value" do
    message = described_class.new(id: "1-0", payload: { "a" => 1 }, serializer: Queekiq::Serializers::JSON)

    expect(message.raw).to eq('{"a":1}')
    expect(message.payload).to eq("a" => 1)
  end

  it "decodes a raw value lazily, so a bad payload only fails when it is used" do
    message = described_class.new(id: "1-0", raw: "{broken", serializer: Queekiq::Serializers::JSON)

    expect(message.raw).to eq("{broken")
    expect { message.payload }.to raise_error(Queekiq::SerializationError)
  end

  it "decodes only once" do
    serializer = double("serializer", load: { "x" => 1 }, dump: "{}")
    message = described_class.new(id: "1-0", raw: "{}", serializer: serializer)

    2.times { message.payload }

    expect(serializer).to have_received(:load).once
  end

  it "accepts nil as a real payload" do
    message = described_class.new(id: "1-0", payload: nil, raw: "null", serializer: Queekiq::Serializers::JSON)

    expect(message.payload).to be_nil
  end

  describe "#latency_ms" do
    it "is the time since the message was first enqueued" do
      message = described_class.new(id: "1-0", payload: "x", enqueued_at: 1_000)

      expect(message.latency_ms(1_250)).to eq(250)
    end

    it "is nil when the enqueue time is unknown" do
      expect(described_class.new(id: "1-0", payload: "x").latency_ms).to be_nil
    end
  end

  it "has a short inspect" do
    message = described_class.new(id: "1-0", payload: "x" * 500)

    expect(message.inspect.size).to be < 150
  end
end
