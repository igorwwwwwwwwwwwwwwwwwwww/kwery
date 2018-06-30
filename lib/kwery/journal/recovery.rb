require 'json'
require 'thread'
require 'socket'

# TODO: shorten the namespacing here
#       maybe move replication to replication/client

module Kwery
  class Journal
    module Recovery
      class File
        def initialize(journal_file:)
          @journal_file = journal_file
        end

        def recover
          file = ::File.open(@journal_file, 'a+')
          file.each_line.map do |line|
            JSON.parse(line, symbolize_names: true)
          end
        end
      end

      class Replication
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
end
