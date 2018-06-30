require 'json'
require 'thread'

# TODO separate write-ahead from replication?

# write-ahead ("redo") and replication log
module Kwery
  class Log
    def initialize(log_file: nil, noop: false)
      @log_file = log_file
      @noop = noop
      @m = Mutex.new
    end

    def append(op, payload)
      return if @noop
      @m.synchronize {
        file << JSON.dump([op, payload]) + "\n"
      }
    end

    def flush
      return if @noop
      @m.synchronize {
        file.fsync
      }
    end

    def file
      @file ||= File.open(@log_file, 'a+')
    end

    def start_flush_thread(sleep_interval: 2)
      Thread.new {
        loop {
          sleep sleep_interval
          flush
        }
      }
    end
  end
end
