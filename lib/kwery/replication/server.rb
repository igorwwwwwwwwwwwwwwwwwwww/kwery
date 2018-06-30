# TODO: switch data sources when replica has caught up?
# TODO: use async i/o to handle this stuff better?

module Kwery
  module Replication
    class Server
      def initialize(log_file:, port: 9200, listen_backlog: 10)
        @log_file = log_file
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
        File.open(@log_file, 'r') do |f|
          f.pos = offset
          until f.eof?
            client << f.read(1024)
          end
          while true do
            select([f])
            client << f.read(1024)
          end
          client.flush
        end
      end
    end
  end
end
