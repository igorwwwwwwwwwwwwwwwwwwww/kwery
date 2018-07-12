require 'json'

module Kwery
  module Format
    class Json
      def load(file, context = nil)
        file.each_line.map do |line|
          JSON.parse(line, symbolize_names: true)
        end
      end

      def encode(tups)
        tups.map do |tup|
          # encode returns a stream of chunks, thus
          # trailing newlines must be included
          tup.to_json + "\n"
        end
      end
    end
  end
end
