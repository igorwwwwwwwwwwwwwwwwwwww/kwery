require 'typhoeus'
require 'json'

module Kwery
  module Client
    class Batch
      def query(queries, client_opts = {}, context = nil)
        hydra = Typhoeus::Hydra.new

        headers = {}
        headers['Partial'] = 'true' if client_opts[:partial]

        # body can be sql or { query: sql, data: stdin }
        reqs = queries.map do |backend, body|
          Typhoeus::Request.new(
            "#{backend}/query",
            method: :post,
            body: body,
            headers: headers,
            verbose: ENV['DEBUG_TYPHOEUS'] == 'true',
          )
        end

        reqs.each do |req|
          context.increment(:backend_requests) if context
          hydra.queue(req)
        end
        hydra.run

        reqs.map do |req|
          unless req.response.success?
            if req.response.body
              begin
                result = JSON.parse(req.response.body, symbolize_names:true)
                error = result[:error]
              rescue
                error = nil
              end
            end
            # TODO expose nested exception metadata in structured form
            raise "error response for request #{req.url} status=#{req.response.code} timeout=#{req.response.timed_out?} body=\"#{req.options[:body]}\" error=\"#{error}\""
          end
          JSON.parse(req.response.body, symbolize_names: true)
        end
      end
    end
  end
end
