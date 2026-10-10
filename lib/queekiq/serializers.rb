require "json"

module Queekiq
  # A serializer is any object with `dump(object) -> String` and
  # `load(String) -> object`. Queue#enqueue stores what `dump` returns;
  # Message#payload returns what `load` makes of it.
  module Serializers
    # Stores payloads as plain strings (`to_s`) and hands them back untouched.
    # This is the default and matches how the stream looks without Queekiq.
    module Raw
      def self.dump(object)
        object.to_s
      end

      def self.load(string)
        string
      end
    end

    # Stores payloads as JSON, so hashes, arrays, numbers etc. round-trip.
    # Hash keys come back as strings.
    module JSON
      def self.dump(object)
        ::JSON.generate(object)
      rescue ::JSON::JSONError => e
        raise SerializationError, "can't serialize #{object.class} as JSON: #{e.message}"
      end

      def self.load(string)
        ::JSON.parse(string)
      rescue ::JSON::JSONError => e
        raise SerializationError, "payload is not valid JSON: #{e.message}"
      end
    end
  end
end
