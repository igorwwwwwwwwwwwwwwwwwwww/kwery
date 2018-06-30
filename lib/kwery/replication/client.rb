require 'json'
require 'socket'

module Kwery
  module Replication
    class Client
      def initialize(primary:, offset: 0)
        @primary = primary
        @offset = offset
      end

      def recover
        host, port = @primary.split(':')
        client = TCPSocket.new host, port
        client.write("#{@offset}\n")

        client.each_line.lazy.map do |line|
          JSON.parse(line, symbolize_names: true)
        end
      end
    end
  end
end
