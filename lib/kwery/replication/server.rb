# TODO: use async i/o to handle this stuff better?
# TODO: replicate via raft? (consensus)
# TODO: still support oplog-style change stream?

module Kwery
  module Replication
    class Server
      def initialize(journal:, journal_file:, port: 9200, listen_backlog: 10)
        @journal = journal
        @journal_file = journal_file
        @port = port
        @listen_backlog = listen_backlog
      end

      def listen
        Thread.new {
          server = TCPServer.new @port
          server.listen(@listen_backlog)

          loop do
            client = server.accept
            Thread.new {
              handle_client(client)
            }
          end
        }
      end

      def handle_client(client)
        offset = client.gets&.to_i
        File.open(@journal_file, 'r') do |f|
          f.pos = offset
          until f.eof?
            client << f.read(1024)
          end
          client.flush
        end
        # replay complete, handoff to log writer
        @journal.register_client(client)
      end
    end
  end
end
