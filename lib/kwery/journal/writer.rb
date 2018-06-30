require 'json'
require 'thread'

# TODO separate write-ahead from replication?

# write-ahead ("redo") and replication log
module Kwery
  module Journal
    class Writer
      def initialize(journal_file: nil)
        @journal_file = journal_file
        @m = Mutex.new
      end

      def append(op, payload)
        @m.synchronize {
          file << JSON.dump([op, payload]) + "\n"
        }
      end

      def flush
        @m.synchronize {
          file.fsync
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
        @file ||= File.open(@journal_file, 'a+')
      end
    end

    class NoopWriter
      def append(op, payload)
      end

      def flush
      end

      def start_flush_thread(**kwargs)
      end
    end
  end
end
