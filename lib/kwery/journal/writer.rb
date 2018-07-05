require 'json'
require 'thread'

# TODO further separate write-ahead from replication?
# TODO handle disconnected clients
# TODO check tx offset (lsn) during register_client handoff

# write-ahead ("redo") and replication log
module Kwery
  module Journal
    class Writer
      def initialize(journal_file: nil)
        @journal_file = journal_file
        @clients = []
        @m = Mutex.new
      end

      def register_client(client)
        @m.synchronize {
          @clients << client
        }
      end

      def append(op, payload)
        @m.synchronize {
          tx = JSON.dump([op, payload]) + "\n"
          file << tx
          @clients.each do |client|
            client << tx
            client.flush
          end
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
