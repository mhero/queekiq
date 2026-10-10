require "spec_helper"

RSpec.describe Queekiq::Serializers do
  describe "Raw" do
    subject(:serializer) { described_class::Raw }

    it "stores objects as strings and returns strings untouched" do
      expect(serializer.dump(42)).to eq("42")
      expect(serializer.dump("abc")).to eq("abc")
      expect(serializer.load("abc")).to eq("abc")
    end
  end

  describe "JSON" do
    subject(:serializer) { described_class::JSON }

    it "round-trips structured data (hash keys come back as strings)" do
      data = { "id" => 7, "tags" => %w[a b], "nested" => { "ok" => true }, "none" => nil }

      expect(serializer.load(serializer.dump(data))).to eq(data)
      expect(serializer.load(serializer.dump(id: 7))).to eq("id" => 7)
    end

    it "round-trips scalars, including nil" do
      [ "text", 12, 1.5, true, nil ].each do |value|
        expect(serializer.load(serializer.dump(value))).to eq(value)
      end
    end

    it "raises SerializationError for invalid JSON" do
      expect { serializer.load("not json {") }.to raise_error(Queekiq::SerializationError, /not valid JSON/)
    end

    it "raises SerializationError for objects it can't encode" do
      expect { serializer.dump(Float::NAN) }.to raise_error(Queekiq::SerializationError, /can't serialize/)
    end
  end
end
