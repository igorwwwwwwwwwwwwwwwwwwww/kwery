require 'json'

module Kwery
  module Journal
    class Recovery
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
  end
end
