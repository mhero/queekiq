module Queekiq
  # One delivery from a queue. `id` is the Redis stream entry id (needed to
  # ack); `payload` is the string that was passed to Queue#enqueue.
  Message = Struct.new(:id, :payload, keyword_init: true)
end
