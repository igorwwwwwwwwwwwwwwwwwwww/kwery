require 'json'
require 'thread'

# write-ahead ("redo") and replication log

module Kwery
  class Log
    def initialize(filename = nil)
      @buffer = []
      @filename = filename
      @m = Mutex.new
    end

    def recover
      file.each_line.map do |line|
        JSON.parse(line, symbolize_names: true)
      end
    end

    def append(op, payload)
      @m.synchronize {
        @buffer << JSON.dump([op, payload]) + "\n"
      }
    end

    def flush
      @m.synchronize {
        if file && @buffer.size > 0
          @buffer.each do |tx|
            file << tx
          end
          file.fsync
        end
        @buffer = []
      }
    end

    def start_flush_thread(sleep_interval: 2)
      Thread.new {
        loop {
          sleep sleep_interval
          flush
        }
      }
    end

    private

    def file
      return unless @filename

      @file ||= File.open(@filename, 'a+')
    end
  end
end
